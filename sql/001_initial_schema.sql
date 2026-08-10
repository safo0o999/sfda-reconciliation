/*
    SFDA Reconciliation V6
    Targeted schema repair: missing ExpiryMonthKey

    Purpose
    -------
    Repair only existing V6 tables that should contain ExpiryMonthKey but do
    not currently have it. Existing values are preserved. Missing values are
    backfilled from ExpiryDate as YYYY-MM.

    This script does NOT run the full schema migration and does NOT delete data.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    /* Let the admin/migration connection see all warehouses under the V6 RLS
       predicate design. */
    EXEC sys.sp_set_session_context @key=N'WarehouseID', @value=NULL;

    /* -----------------------------------------------------------
       ReceiptEvents
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.ReceiptEvents', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.ReceiptEvents', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding ReceiptEvents.ExpiryMonthKey';
        ALTER TABLE dbo.ReceiptEvents ADD ExpiryMonthKey char(7) NULL;

        UPDATE dbo.ReceiptEvents
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.ReceiptEvents WHERE ExpiryMonthKey IS NULL)
            THROW 52001, 'ReceiptEvents contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.ReceiptEvents ALTER COLUMN ExpiryMonthKey char(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       DispatchEvents
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.DispatchEvents', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.DispatchEvents', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding DispatchEvents.ExpiryMonthKey';
        ALTER TABLE dbo.DispatchEvents ADD ExpiryMonthKey char(7) NULL;

        UPDATE dbo.DispatchEvents
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.DispatchEvents WHERE ExpiryMonthKey IS NULL)
            THROW 52002, 'DispatchEvents contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.DispatchEvents ALTER COLUMN ExpiryMonthKey char(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       BatchMaster
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.BatchMaster', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.BatchMaster', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding BatchMaster.ExpiryMonthKey';
        ALTER TABLE dbo.BatchMaster ADD ExpiryMonthKey char(7) NULL;

        UPDATE dbo.BatchMaster
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.BatchMaster WHERE ExpiryMonthKey IS NULL)
            THROW 52003, 'BatchMaster contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.BatchMaster ALTER COLUMN ExpiryMonthKey char(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       SupplierHistory
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.SupplierHistory', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.SupplierHistory', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding SupplierHistory.ExpiryMonthKey';
        ALTER TABLE dbo.SupplierHistory ADD ExpiryMonthKey char(7) NULL;

        UPDATE dbo.SupplierHistory
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.SupplierHistory WHERE ExpiryMonthKey IS NULL)
            THROW 52004, 'SupplierHistory contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.SupplierHistory ALTER COLUMN ExpiryMonthKey char(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       CustomerHistory
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.CustomerHistory', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.CustomerHistory', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding CustomerHistory.ExpiryMonthKey';
        ALTER TABLE dbo.CustomerHistory ADD ExpiryMonthKey char(7) NULL;

        UPDATE dbo.CustomerHistory
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.CustomerHistory WHERE ExpiryMonthKey IS NULL)
            THROW 52005, 'CustomerHistory contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.CustomerHistory ALTER COLUMN ExpiryMonthKey char(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       LatestInventorySnapshot
       Snapshot column is nullable by design.
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.LatestInventorySnapshot', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.LatestInventorySnapshot', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding LatestInventorySnapshot.ExpiryMonthKey';
        ALTER TABLE dbo.LatestInventorySnapshot ADD ExpiryMonthKey nvarchar(20) NULL;

        UPDATE dbo.LatestInventorySnapshot
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;
    END;

    /* -----------------------------------------------------------
       LatestSFDASnapshot
       Snapshot column is nullable by design.
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.LatestSFDASnapshot', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.LatestSFDASnapshot', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding LatestSFDASnapshot.ExpiryMonthKey';
        ALTER TABLE dbo.LatestSFDASnapshot ADD ExpiryMonthKey nvarchar(20) NULL;

        UPDATE dbo.LatestSFDASnapshot
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;
    END;

    /* -----------------------------------------------------------
       DailyAcceptTransactions
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.DailyAcceptTransactions', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.DailyAcceptTransactions', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding DailyAcceptTransactions.ExpiryMonthKey';
        ALTER TABLE dbo.DailyAcceptTransactions ADD ExpiryMonthKey nvarchar(7) NULL;

        UPDATE dbo.DailyAcceptTransactions
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.DailyAcceptTransactions WHERE ExpiryMonthKey IS NULL)
            THROW 52008, 'DailyAcceptTransactions contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.DailyAcceptTransactions ALTER COLUMN ExpiryMonthKey nvarchar(7) NOT NULL;
    END;

    /* -----------------------------------------------------------
       FullDispatchTransactions
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.FullDispatchTransactions', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.FullDispatchTransactions', N'ExpiryMonthKey') IS NULL
    BEGIN
        PRINT N'Adding FullDispatchTransactions.ExpiryMonthKey';
        ALTER TABLE dbo.FullDispatchTransactions ADD ExpiryMonthKey nvarchar(7) NULL;

        UPDATE dbo.FullDispatchTransactions
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL
          AND ExpiryDate IS NOT NULL;

        IF EXISTS (SELECT 1 FROM dbo.FullDispatchTransactions WHERE ExpiryMonthKey IS NULL)
            THROW 52009, 'FullDispatchTransactions contains rows that cannot derive ExpiryMonthKey from ExpiryDate.', 1;

        ALTER TABLE dbo.FullDispatchTransactions ALTER COLUMN ExpiryMonthKey nvarchar(7) NOT NULL;
    END;

    /* Useful lookup indexes - create only when missing. */
    IF OBJECT_ID(N'dbo.SupplierHistory', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.SupplierHistory', N'ExpiryMonthKey') IS NOT NULL
       AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'dbo.SupplierHistory')
              AND name = N'IX_SupplierHistory_Lookup'
       )
    BEGIN
        CREATE INDEX IX_SupplierHistory_Lookup
            ON dbo.SupplierHistory (SupplierCode, GenericItemNumber, BN, ExpiryMonthKey);
    END;

    IF OBJECT_ID(N'dbo.CustomerHistory', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.CustomerHistory', N'ExpiryMonthKey') IS NOT NULL
       AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'dbo.CustomerHistory')
              AND name = N'IX_CustomerHistory_Lookup'
       )
    BEGIN
        CREATE INDEX IX_CustomerHistory_Lookup
            ON dbo.CustomerHistory (GLN, GenericItemNumber, BN, ExpiryMonthKey);
    END;

    IF OBJECT_ID(N'dbo.FullDispatchTransactions', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.FullDispatchTransactions', N'ExpiryMonthKey') IS NOT NULL
       AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
              AND name = N'IX_FullDispatchTransactions_Batch'
       )
    BEGIN
        CREATE INDEX IX_FullDispatchTransactions_Batch
            ON dbo.FullDispatchTransactions (BN, ExpiryMonthKey, CreatedAt);
    END;

    COMMIT TRANSACTION;

    /* Post-check: show every relevant table and whether the column exists. */
    SELECT
        v.TableName,
        CASE WHEN COL_LENGTH(N'dbo.' + v.TableName, N'ExpiryMonthKey') IS NOT NULL
             THEN N'OK'
             ELSE N'MISSING'
        END AS ExpiryMonthKeyStatus
    FROM (VALUES
        (N'ReceiptEvents'),
        (N'DispatchEvents'),
        (N'BatchMaster'),
        (N'SupplierHistory'),
        (N'CustomerHistory'),
        (N'LatestInventorySnapshot'),
        (N'LatestSFDASnapshot'),
        (N'DailyAcceptTransactions'),
        (N'FullDispatchTransactions')
    ) v(TableName)
    WHERE OBJECT_ID(N'dbo.' + v.TableName, N'U') IS NOT NULL
    ORDER BY v.TableName;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    SELECT
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_LINE() AS ErrorLine,
        ERROR_MESSAGE() AS ErrorMessage;

    THROW;
END CATCH;
