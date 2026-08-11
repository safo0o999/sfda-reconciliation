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


    /* ===========================================================
       Multi-Warehouse isolation hardening
       ===========================================================

       IMPORTANT:
       - Existing business data is preserved.
       - Existing WarehouseID values are preserved.
       - No reconciliation calculations are changed.
       - Baseline business keys become warehouse-aware.
       - Legacy WarehouseID defaults that silently fall back to Warehouse 1
         are replaced with a strict SESSION_CONTEXT-based default.
    */

    /* -----------------------------------------------------------
       Safety check: baseline PKs must not be referenced by foreign keys.
       We stop instead of dropping a referenced key unexpectedly.
       ----------------------------------------------------------- */
    IF EXISTS (
        SELECT 1
        FROM sys.foreign_keys fk
        WHERE fk.referenced_object_id IN (
            OBJECT_ID(N'dbo.DailyAcceptSFDABaseline'),
            OBJECT_ID(N'dbo.DailyDispatchSFDABaseline'),
            OBJECT_ID(N'dbo.FullDispatchSFDABaseline')
        )
    )
    BEGIN
        THROW 52100, 'A foreign key references one of the baseline tables. Review the FK before changing the warehouse-aware primary key.', 1;
    END;

    /* -----------------------------------------------------------
       DailyAcceptSFDABaseline
       Old key: (BN, ExpiryDate)
       V6 key : (WarehouseID, BN, ExpiryDate)
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.DailyAcceptSFDABaseline', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.DailyAcceptSFDABaseline', N'WarehouseID') IS NULL
            THROW 52101, 'DailyAcceptSFDABaseline.WarehouseID is missing.', 1;

        IF EXISTS (
            SELECT WarehouseID, BN, ExpiryDate
            FROM dbo.DailyAcceptSFDABaseline
            GROUP BY WarehouseID, BN, ExpiryDate
            HAVING COUNT_BIG(*) > 1
        )
            THROW 52102, 'DailyAcceptSFDABaseline contains duplicate WarehouseID + BN + ExpiryDate rows.', 1;

        IF EXISTS (
            SELECT 1
            FROM sys.key_constraints kc
            WHERE kc.parent_object_id = OBJECT_ID(N'dbo.DailyAcceptSFDABaseline')
              AND kc.type = 'PK'
              AND NOT EXISTS (
                  SELECT 1
                  FROM sys.index_columns ic
                  JOIN sys.columns c
                    ON c.object_id = ic.object_id
                   AND c.column_id = ic.column_id
                  WHERE ic.object_id = kc.parent_object_id
                    AND ic.index_id = kc.unique_index_id
                    AND c.name = N'WarehouseID'
              )
        )
        BEGIN
            ALTER TABLE dbo.DailyAcceptSFDABaseline
                DROP CONSTRAINT PK_DailyAcceptSFDABaseline;

            ALTER TABLE dbo.DailyAcceptSFDABaseline
                ADD CONSTRAINT PK_DailyAcceptSFDABaseline
                PRIMARY KEY (WarehouseID, BN, ExpiryDate);
        END;
    END;

    /* -----------------------------------------------------------
       DailyDispatchSFDABaseline
       Old key: (BN, ExpiryDate)
       V6 key : (WarehouseID, BN, ExpiryDate)
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.DailyDispatchSFDABaseline', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.DailyDispatchSFDABaseline', N'WarehouseID') IS NULL
            THROW 52103, 'DailyDispatchSFDABaseline.WarehouseID is missing.', 1;

        IF EXISTS (
            SELECT WarehouseID, BN, ExpiryDate
            FROM dbo.DailyDispatchSFDABaseline
            GROUP BY WarehouseID, BN, ExpiryDate
            HAVING COUNT_BIG(*) > 1
        )
            THROW 52104, 'DailyDispatchSFDABaseline contains duplicate WarehouseID + BN + ExpiryDate rows.', 1;

        IF EXISTS (
            SELECT 1
            FROM sys.key_constraints kc
            WHERE kc.parent_object_id = OBJECT_ID(N'dbo.DailyDispatchSFDABaseline')
              AND kc.type = 'PK'
              AND NOT EXISTS (
                  SELECT 1
                  FROM sys.index_columns ic
                  JOIN sys.columns c
                    ON c.object_id = ic.object_id
                   AND c.column_id = ic.column_id
                  WHERE ic.object_id = kc.parent_object_id
                    AND ic.index_id = kc.unique_index_id
                    AND c.name = N'WarehouseID'
              )
        )
        BEGIN
            ALTER TABLE dbo.DailyDispatchSFDABaseline
                DROP CONSTRAINT PK_DailyDispatchSFDABaseline;

            ALTER TABLE dbo.DailyDispatchSFDABaseline
                ADD CONSTRAINT PK_DailyDispatchSFDABaseline
                PRIMARY KEY (WarehouseID, BN, ExpiryDate);
        END;
    END;

    /* -----------------------------------------------------------
       FullDispatchSFDABaseline
       Old key: (BN, ExpiryDate)
       V6 key : (WarehouseID, BN, ExpiryDate)
       ----------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.FullDispatchSFDABaseline', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.FullDispatchSFDABaseline', N'WarehouseID') IS NULL
            THROW 52105, 'FullDispatchSFDABaseline.WarehouseID is missing.', 1;

        IF EXISTS (
            SELECT WarehouseID, BN, ExpiryDate
            FROM dbo.FullDispatchSFDABaseline
            GROUP BY WarehouseID, BN, ExpiryDate
            HAVING COUNT_BIG(*) > 1
        )
            THROW 52106, 'FullDispatchSFDABaseline contains duplicate WarehouseID + BN + ExpiryDate rows.', 1;

        IF EXISTS (
            SELECT 1
            FROM sys.key_constraints kc
            WHERE kc.parent_object_id = OBJECT_ID(N'dbo.FullDispatchSFDABaseline')
              AND kc.type = 'PK'
              AND NOT EXISTS (
                  SELECT 1
                  FROM sys.index_columns ic
                  JOIN sys.columns c
                    ON c.object_id = ic.object_id
                   AND c.column_id = ic.column_id
                  WHERE ic.object_id = kc.parent_object_id
                    AND ic.index_id = kc.unique_index_id
                    AND c.name = N'WarehouseID'
              )
        )
        BEGIN
            ALTER TABLE dbo.FullDispatchSFDABaseline
                DROP CONSTRAINT PK_FullDispatchSFDABaseline;

            ALTER TABLE dbo.FullDispatchSFDABaseline
                ADD CONSTRAINT PK_FullDispatchSFDABaseline
                PRIMARY KEY (WarehouseID, BN, ExpiryDate);
        END;
    END;

    /* -----------------------------------------------------------
       Remove unsafe Warehouse 1 fallback from operational tables.

       Old default example:
           ISNULL(TRY_CAST(SESSION_CONTEXT(N'WarehouseID') AS int), 1)

       New default:
           TRY_CAST(SESSION_CONTEXT(N'WarehouseID') AS int)

       Because WarehouseID is NOT NULL on protected operational tables,
       a connection with no WarehouseID context now FAILS instead of
       silently writing data into Warehouse 1 / Madinah.
       ----------------------------------------------------------- */
    DECLARE @WarehouseDefaultTables TABLE (TableName sysname PRIMARY KEY);

    INSERT INTO @WarehouseDefaultTables (TableName)
    VALUES
        (N'BatchEvents'),
        (N'BatchMaster'),
        (N'CustomerHistory'),
        (N'DailyAcceptSFDABaseline'),
        (N'DailyAcceptTransactions'),
        (N'DailyDispatchConfirmations'),
        (N'DailyDispatchSFDABaseline'),
        (N'DailyDispatchTransactions'),
        (N'DailyProcessedTransactions'),
        (N'DispatchEvents'),
        (N'FullDispatchConfirmations'),
        (N'FullDispatchEvents'),
        (N'FullDispatchSFDABaseline'),
        (N'FullDispatchTransactions'),
        (N'FullReceiptEvents'),
        (N'FullReconciliationRuns'),
        (N'HistoricalBuildJobs'),
        (N'LatestInventorySnapshot'),
        (N'LatestSFDASnapshot'),
        (N'OutlookDraftRequests'),
        (N'ReceiptEvents'),
        (N'ReconciliationRunFiles'),
        (N'ReconciliationRuns'),
        (N'RunHistory'),
        (N'SupplierHistory');

    DECLARE
        @TableName sysname,
        @ConstraintName sysname,
        @Sql nvarchar(max);

    DECLARE warehouse_default_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT d.TableName
        FROM @WarehouseDefaultTables d
        WHERE OBJECT_ID(N'dbo.' + d.TableName, N'U') IS NOT NULL
          AND COL_LENGTH(N'dbo.' + d.TableName, N'WarehouseID') IS NOT NULL;

    OPEN warehouse_default_cursor;
    FETCH NEXT FROM warehouse_default_cursor INTO @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @ConstraintName = NULL;

        SELECT @ConstraintName = dc.name
        FROM sys.default_constraints dc
        JOIN sys.columns c
          ON c.object_id = dc.parent_object_id
         AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'dbo.' + @TableName)
          AND c.name = N'WarehouseID';

        IF @ConstraintName IS NOT NULL
        BEGIN
            SET @Sql = N'ALTER TABLE dbo.' + QUOTENAME(@TableName)
                     + N' DROP CONSTRAINT ' + QUOTENAME(@ConstraintName) + N';';
            EXEC sys.sp_executesql @Sql;
        END;

        SET @ConstraintName = N'DF_' + @TableName + N'_WarehouseID';

        SET @Sql = N'ALTER TABLE dbo.' + QUOTENAME(@TableName)
                 + N' ADD CONSTRAINT ' + QUOTENAME(@ConstraintName)
                 + N' DEFAULT (TRY_CAST(SESSION_CONTEXT(N''WarehouseID'') AS int)) FOR WarehouseID;';
        EXEC sys.sp_executesql @Sql;

        FETCH NEXT FROM warehouse_default_cursor INTO @TableName;
    END;

    CLOSE warehouse_default_cursor;
    DEALLOCATE warehouse_default_cursor;

    /* -----------------------------------------------------------
       Required RLS verification for the three warehouse-aware
       baseline tables.  Migration stops if isolation is incomplete.
       ----------------------------------------------------------- */
    DECLARE @BaselineRls TABLE (TableName sysname PRIMARY KEY);
    INSERT INTO @BaselineRls (TableName)
    VALUES
        (N'DailyAcceptSFDABaseline'),
        (N'DailyDispatchSFDABaseline'),
        (N'FullDispatchSFDABaseline');

    IF EXISTS (
        SELECT 1
        FROM @BaselineRls b
        WHERE OBJECT_ID(N'dbo.' + b.TableName, N'U') IS NOT NULL
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM sys.security_predicates spr
                  JOIN sys.security_policies sp
                    ON sp.object_id = spr.object_id
                  WHERE spr.target_object_id = OBJECT_ID(N'dbo.' + b.TableName)
                    AND sp.is_enabled = 1
                    AND spr.predicate_type_desc = N'FILTER'
              )
              OR NOT EXISTS (
                  SELECT 1
                  FROM sys.security_predicates spr
                  JOIN sys.security_policies sp
                    ON sp.object_id = spr.object_id
                  WHERE spr.target_object_id = OBJECT_ID(N'dbo.' + b.TableName)
                    AND sp.is_enabled = 1
                    AND spr.predicate_type_desc = N'BLOCK'
                    AND spr.operation_desc = N'AFTER INSERT'
              )
              OR NOT EXISTS (
                  SELECT 1
                  FROM sys.security_predicates spr
                  JOIN sys.security_policies sp
                    ON sp.object_id = spr.object_id
                  WHERE spr.target_object_id = OBJECT_ID(N'dbo.' + b.TableName)
                    AND sp.is_enabled = 1
                    AND spr.predicate_type_desc = N'BLOCK'
                    AND spr.operation_desc = N'AFTER UPDATE'
              )
          )
    )
        THROW 52107, 'Warehouse RLS is incomplete on one or more baseline tables.', 1;

    /* -----------------------------------------------------------
       Final PK verification before commit.
       ----------------------------------------------------------- */
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            (N'DailyAcceptSFDABaseline'),
            (N'DailyDispatchSFDABaseline'),
            (N'FullDispatchSFDABaseline')
        ) v(TableName)
        WHERE OBJECT_ID(N'dbo.' + v.TableName, N'U') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM sys.key_constraints kc
              JOIN sys.index_columns ic
                ON ic.object_id = kc.parent_object_id
               AND ic.index_id = kc.unique_index_id
              JOIN sys.columns c
                ON c.object_id = ic.object_id
               AND c.column_id = ic.column_id
              WHERE kc.parent_object_id = OBJECT_ID(N'dbo.' + v.TableName)
                AND kc.type = N'PK'
                AND c.name = N'WarehouseID'
          )
    )
        THROW 52108, 'WarehouseID is still missing from a baseline primary key.', 1;

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

    /* Multi-Warehouse isolation verification output. */
    SELECT
        t.name AS TableName,
        kc.name AS PrimaryKeyName,
        STRING_AGG(c.name, N', ')
            WITHIN GROUP (ORDER BY ic.key_ordinal) AS PrimaryKeyColumns
    FROM sys.tables t
    JOIN sys.key_constraints kc
      ON kc.parent_object_id = t.object_id
     AND kc.type = N'PK'
    JOIN sys.index_columns ic
      ON ic.object_id = kc.parent_object_id
     AND ic.index_id = kc.unique_index_id
     AND ic.key_ordinal > 0
    JOIN sys.columns c
      ON c.object_id = ic.object_id
     AND c.column_id = ic.column_id
    WHERE t.name IN (
        N'DailyAcceptSFDABaseline',
        N'DailyDispatchSFDABaseline',
        N'FullDispatchSFDABaseline'
    )
    GROUP BY t.name, kc.name
    ORDER BY t.name;

    SELECT
        t.name AS TableName,
        dc.name AS WarehouseDefaultConstraint,
        dc.definition AS WarehouseDefaultDefinition
    FROM sys.tables t
    JOIN sys.columns c
      ON c.object_id = t.object_id
     AND c.name = N'WarehouseID'
    LEFT JOIN sys.default_constraints dc
      ON dc.parent_object_id = c.object_id
     AND dc.parent_column_id = c.column_id
    WHERE t.name IN (
        N'DailyAcceptSFDABaseline',
        N'DailyDispatchSFDABaseline',
        N'FullDispatchSFDABaseline',
        N'LatestInventorySnapshot',
        N'LatestSFDASnapshot'
    )
    ORDER BY t.name;

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
