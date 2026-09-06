/*
    Add a non-destructive Cancel Dispatch stream to the existing, already
    warehouse-isolated FullDispatchTransactions ledger.

    Existing rows remain ordinary DISPATCH rows. No historical quantities are
    deleted or rewritten.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.FullDispatchTransactions', N'U') IS NULL
        THROW 52200, 'FullDispatchTransactions must exist before installing Cancel Dispatch.', 1;

    IF COL_LENGTH(N'dbo.FullDispatchTransactions', N'TransactionType') IS NULL
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.FullDispatchTransactions
                ADD TransactionType nvarchar(32) NULL;
        ';

    -- Dynamic SQL is required on first installation. SQL Server compiles a
    -- static reference to TransactionType before the preceding ALTER TABLE is
    -- executed and otherwise raises "Invalid column name TransactionType".
    EXEC sys.sp_executesql N'
        UPDATE dbo.FullDispatchTransactions
        SET TransactionType = N''DISPATCH''
        WHERE TransactionType IS NULL;
    ';

    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
          AND name = N'TransactionType'
          AND is_nullable = 1
    )
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.FullDispatchTransactions
                ALTER COLUMN TransactionType nvarchar(32) NOT NULL;
        ';

    IF NOT EXISTS (
        SELECT 1
        FROM sys.default_constraints dc
        JOIN sys.columns c
          ON c.object_id = dc.parent_object_id
         AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
          AND c.name = N'TransactionType'
    )
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.FullDispatchTransactions
                ADD CONSTRAINT DF_FullDispatchTransactions_TransactionType
                DEFAULT (N''DISPATCH'') FOR TransactionType;
        ';

    IF COL_LENGTH(N'dbo.FullDispatchTransactions', N'SourceType') IS NULL
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.FullDispatchTransactions
                ADD SourceType nvarchar(64) NULL;
        ';

    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
          AND name = N'IX_FullDispatchTransactions_TypeBatch'
    )
    BEGIN
        EXEC sys.sp_executesql N'
            CREATE INDEX IX_FullDispatchTransactions_TypeBatch
                ON dbo.FullDispatchTransactions
                   (TransactionType, BN, ExpiryMonthKey, CreatedAt);
        ';
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
