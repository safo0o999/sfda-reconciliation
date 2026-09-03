/*
    Trade Code history backfill

    Set @WarehouseID before execution. The script repairs only the warehouse's
    active BuildID, uses ReceiptEvents/DispatchEvents as the source of truth,
    and rolls back if supplier or customer quantities change.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @WarehouseID int = 2;
DECLARE @BuildID nvarchar(255);
DECLARE @ExpectedSupplierQty decimal(38, 6);
DECLARE @ExpectedCustomerQty decimal(38, 6);
DECLARE @ActualSupplierQty decimal(38, 6);
DECLARE @ActualCustomerQty decimal(38, 6);

EXEC sys.sp_set_session_context @key=N'WarehouseID', @value=@WarehouseID;

SELECT TOP (1) @BuildID = BuildID
FROM dbo.HistoricalBuildVersions
WHERE WarehouseID = @WarehouseID
  AND IsActive = 1
ORDER BY ActivatedAt DESC, UpdatedAt DESC;

IF NULLIF(@BuildID, N'') IS NULL
    THROW 52301, 'No active historical BuildID exists for this warehouse.', 1;

EXEC sys.sp_set_session_context @key=N'HistoricalBuildID', @value=@BuildID;
EXEC sys.sp_set_session_context @key=N'HistoricalBuildMaintenance', @value=0;

BEGIN TRY
    BEGIN TRANSACTION;

    SELECT
        BN,
        ExpiryMonthKey,
        GenericItemNumber,
        LEFT(
            STRING_AGG(CONVERT(nvarchar(max), TradeItemNumber), N' | ')
                WITHIN GROUP (ORDER BY SourcePriority, TradeItemNumber),
            255
        ) AS TradeItemNumber
    INTO #BatchTradeCodes
    FROM
    (
        SELECT
            BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber,
            MIN(SourcePriority) AS SourcePriority
        FROM
        (
            SELECT r.BN, r.ExpiryMonthKey, r.GenericItemNumber,
                   NULLIF(LTRIM(RTRIM(r.TradeItemNumber)), N'') AS TradeItemNumber,
                   0 AS SourcePriority
            FROM dbo.ReceiptEvents r
            WHERE r.WarehouseID = @WarehouseID AND r.BuildID = @BuildID
            UNION ALL
            SELECT d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
                   NULLIF(LTRIM(RTRIM(d.TradeItemNumber)), N''),
                   1
            FROM dbo.DispatchEvents d
            WHERE d.WarehouseID = @WarehouseID AND d.BuildID = @BuildID
        ) source_codes
        WHERE TradeItemNumber IS NOT NULL
        GROUP BY BN, ExpiryMonthKey, GenericItemNumber, TradeItemNumber
    ) distinct_codes
    GROUP BY BN, ExpiryMonthKey, GenericItemNumber;

    CREATE UNIQUE CLUSTERED INDEX IX_BatchTradeCodes
        ON #BatchTradeCodes (BN, ExpiryMonthKey, GenericItemNumber);

    UPDATE bm
    SET bm.TradeItemNumber = tc.TradeItemNumber,
        bm.LastUpdated = SYSUTCDATETIME()
    FROM dbo.BatchMaster bm
    INNER JOIN #BatchTradeCodes tc
        ON tc.BN = bm.BN
       AND tc.ExpiryMonthKey = bm.ExpiryMonthKey
       AND tc.GenericItemNumber = bm.GenericItemNumber
    WHERE bm.WarehouseID = @WarehouseID
      AND bm.BuildID = @BuildID
      AND ISNULL(bm.TradeItemNumber, N'') <> ISNULL(tc.TradeItemNumber, N'');

    SELECT @ExpectedSupplierQty = COALESCE(SUM(r.ReceivedQuantity), 0)
    FROM dbo.ReceiptEvents r
    INNER JOIN dbo.BatchMaster bm
        ON bm.WarehouseID = r.WarehouseID AND bm.BuildID = r.BuildID
       AND bm.BN = r.BN AND bm.ExpiryMonthKey = r.ExpiryMonthKey
       AND bm.GenericItemNumber = r.GenericItemNumber
    WHERE r.WarehouseID = @WarehouseID AND r.BuildID = @BuildID
      AND UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
      AND REPLACE(REPLACE(REPLACE(
            UPPER(LTRIM(RTRIM(ISNULL(r.ItemFamilyGroup, N'')))),
            N' ', N''), N'-', N''), N'_', N'') <> N'LABORATORYSUPPLIES';

    SELECT @ExpectedCustomerQty = COALESCE(SUM(d.DispatchedQuantity), 0)
    FROM dbo.DispatchEvents d
    INNER JOIN dbo.BatchMaster bm
        ON bm.WarehouseID = d.WarehouseID AND bm.BuildID = d.BuildID
       AND bm.BN = d.BN AND bm.ExpiryMonthKey = d.ExpiryMonthKey
       AND bm.GenericItemNumber = d.GenericItemNumber
    WHERE d.WarehouseID = @WarehouseID AND d.BuildID = @BuildID
      AND REPLACE(REPLACE(REPLACE(
            UPPER(LTRIM(RTRIM(ISNULL(d.Custody, N'')))),
            N' ', N''), N'-', N''), N'_', N'') <> N'BIOCHEMICALS';

    DELETE FROM dbo.SupplierHistory
    WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID;

    INSERT INTO dbo.SupplierHistory
    (
        SupplierName, SupplierCode, GTIN, DrugName, GenericItemNumber,
        Description, TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
        PackageSize, ReceivedQuantityEach, ReceivedQuantityPack,
        FirstReceivedDate, LastReceivedDate, ItemFamilyGroup,
        TradeItemNumber, LastUpdated
    )
    SELECT
        r.SupplierName, r.SupplierCode, bm.GTIN, bm.DrugName,
        r.GenericItemNumber,
        COALESCE(NULLIF(MAX(r.Description), N''), bm.Description, N''),
        COALESCE(NULLIF(MAX(r.TradeName), N''), bm.TradeName, N''),
        r.BN, r.ExpiryMonthKey, COALESCE(bm.ExpiryDate, MAX(r.ExpiryDate)),
        CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
        SUM(COALESCE(r.ReceivedQuantity, 0)),
        SUM(COALESCE(r.ReceivedQuantity, 0)) /
            CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
        MIN(r.ReceivedDate), MAX(r.ReceivedDate),
        COALESCE(NULLIF(MAX(r.ItemFamilyGroup), N''), bm.ItemFamilyGroup, N''),
        COALESCE(NULLIF(r.TradeItemNumber, N''), N''), SYSUTCDATETIME()
    FROM dbo.ReceiptEvents r
    INNER JOIN dbo.BatchMaster bm
        ON bm.WarehouseID = r.WarehouseID AND bm.BuildID = r.BuildID
       AND bm.BN = r.BN AND bm.ExpiryMonthKey = r.ExpiryMonthKey
       AND bm.GenericItemNumber = r.GenericItemNumber
    WHERE r.WarehouseID = @WarehouseID AND r.BuildID = @BuildID
      AND UPPER(LTRIM(RTRIM(ISNULL(r.InboundShipment, N'')))) LIKE N'TRK5060%'
      AND REPLACE(REPLACE(REPLACE(
            UPPER(LTRIM(RTRIM(ISNULL(r.ItemFamilyGroup, N'')))),
            N' ', N''), N'-', N''), N'_', N'') <> N'LABORATORYSUPPLIES'
    GROUP BY
        r.SupplierName, r.SupplierCode, r.BN, r.ExpiryMonthKey,
        r.GenericItemNumber, r.TradeItemNumber, bm.GTIN, bm.DrugName,
        bm.Description, bm.TradeName, bm.ExpiryDate, bm.PackageSize,
        bm.ItemFamilyGroup;

    DELETE FROM dbo.CustomerHistory
    WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID;

    INSERT INTO dbo.CustomerHistory
    (
        ToAddress, GLN, GTIN, DrugName, GenericItemNumber,
        TradeDescription, BN, ExpiryMonthKey, ExpiryDate,
        PackageSize, DispatchQuantityEach, DispatchQuantityPack,
        FirstDispatchDate, LastDispatchDate, Custody, TradeItemNumber,
        LastUpdated
    )
    SELECT
        d.ToAddress, N'', bm.GTIN, bm.DrugName, d.GenericItemNumber,
        COALESCE(NULLIF(MAX(d.TradeName), N''), bm.TradeName, N''),
        d.BN, d.ExpiryMonthKey, COALESCE(bm.ExpiryDate, MAX(d.ExpiryDate)),
        CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
        SUM(COALESCE(d.DispatchedQuantity, 0)),
        SUM(COALESCE(d.DispatchedQuantity, 0)) /
            CASE WHEN COALESCE(bm.PackageSize, 0) > 0 THEN bm.PackageSize ELSE 1 END,
        MIN(d.DispatchDate), MAX(d.DispatchDate),
        COALESCE(NULLIF(MAX(d.Custody), N''), NULLIF(MAX(bm.Custody), N''), N''),
        COALESCE(NULLIF(d.TradeItemNumber, N''), N''), SYSUTCDATETIME()
    FROM dbo.DispatchEvents d
    INNER JOIN dbo.BatchMaster bm
        ON bm.WarehouseID = d.WarehouseID AND bm.BuildID = d.BuildID
       AND bm.BN = d.BN AND bm.ExpiryMonthKey = d.ExpiryMonthKey
       AND bm.GenericItemNumber = d.GenericItemNumber
    WHERE d.WarehouseID = @WarehouseID AND d.BuildID = @BuildID
      AND REPLACE(REPLACE(REPLACE(
            UPPER(LTRIM(RTRIM(ISNULL(d.Custody, N'')))),
            N' ', N''), N'-', N''), N'_', N'') <> N'BIOCHEMICALS'
    GROUP BY
        d.ToAddress, d.BN, d.ExpiryMonthKey, d.GenericItemNumber,
        d.TradeItemNumber, bm.GTIN, bm.DrugName, bm.TradeName,
        bm.ExpiryDate, bm.PackageSize;

    SELECT @ActualSupplierQty = COALESCE(SUM(ReceivedQuantityEach), 0)
    FROM dbo.SupplierHistory
    WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID;

    SELECT @ActualCustomerQty = COALESCE(SUM(DispatchQuantityEach), 0)
    FROM dbo.CustomerHistory
    WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID;

    IF @ExpectedSupplierQty <> @ActualSupplierQty
        THROW 52302, 'SupplierHistory quantity verification failed; transaction rolled back.', 1;

    IF @ExpectedCustomerQty <> @ActualCustomerQty
        THROW 52303, 'CustomerHistory quantity verification failed; transaction rolled back.', 1;

    COMMIT TRANSACTION;

    SELECT
        @WarehouseID AS WarehouseID,
        @BuildID AS BuildID,
        @ExpectedSupplierQty AS SupplierQuantity,
        @ExpectedCustomerQty AS CustomerQuantity,
        (SELECT COUNT_BIG(*) FROM dbo.BatchMaster WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID) AS BatchMasterRows,
        (SELECT COUNT_BIG(*) FROM dbo.SupplierHistory WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID) AS SupplierHistoryRows,
        (SELECT COUNT_BIG(*) FROM dbo.CustomerHistory WHERE WarehouseID = @WarehouseID AND BuildID = @BuildID) AS CustomerHistoryRows;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
