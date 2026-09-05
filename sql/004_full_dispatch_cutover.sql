/*
    Establish one warehouse-scoped Full Dispatch cutover.

    The cutover closes all cumulative Customer History that existed when SFDA
    Active was aligned to physical Inventory. Full Dispatch then consumes only
    customer movement added after that point. Existing historical data and
    pre-cutover transaction audit rows are preserved.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.FullDispatchCutovers', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FullDispatchCutovers
        (
            WarehouseID int NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_WarehouseID
                DEFAULT (TRY_CAST(SESSION_CONTEXT(N'WarehouseID') AS int)),
            CutoverID uniqueidentifier NOT NULL,
            Status nvarchar(32) NOT NULL,
            ActivatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_ActivatedAt DEFAULT SYSUTCDATETIME(),
            ActivatedBy nvarchar(320) NULL,
            InventorySourceFile nvarchar(1000) NULL,
            SFDASourceFile nvarchar(1000) NULL,
            CustomerBaselineRows bigint NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_CustomerRows DEFAULT (0),
            CustomerBaselinePack decimal(38,6) NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_CustomerPack DEFAULT (0),
            AlignmentRows bigint NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_AlignmentRows DEFAULT (0),
            MaxDifferencePack decimal(38,6) NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_MaxDifference DEFAULT (0),
            CreatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchCutovers_CreatedAt DEFAULT SYSUTCDATETIME(),
            CONSTRAINT PK_FullDispatchCutovers
                PRIMARY KEY (WarehouseID, CutoverID),
            CONSTRAINT CK_FullDispatchCutovers_Status
                CHECK (Status IN (N'Active', N'Superseded'))
        );

        CREATE UNIQUE INDEX UX_FullDispatchCutovers_ActiveWarehouse
            ON dbo.FullDispatchCutovers (WarehouseID)
            WHERE Status = N'Active';
    END;

    IF OBJECT_ID(N'dbo.FullDispatchCutoverBaseline', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FullDispatchCutoverBaseline
        (
            BaselineID bigint IDENTITY(1,1) NOT NULL
                CONSTRAINT PK_FullDispatchCutoverBaseline PRIMARY KEY,
            WarehouseID int NOT NULL
                CONSTRAINT DF_FullDispatchCutoverBaseline_WarehouseID
                DEFAULT (TRY_CAST(SESSION_CONTEXT(N'WarehouseID') AS int)),
            CutoverID uniqueidentifier NOT NULL,
            BN nvarchar(255) NOT NULL,
            ExpiryMonthKey nvarchar(7) NOT NULL,
            GenericItemNumber nvarchar(255) NOT NULL,
            ToAddress nvarchar(1000) NOT NULL,
            GLN nvarchar(100) NULL,
            PackageSize decimal(38,6) NOT NULL,
            ClosedQuantityEach decimal(38,6) NOT NULL,
            ClosedQuantityPack decimal(38,6) NOT NULL,
            CreatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchCutoverBaseline_CreatedAt DEFAULT SYSUTCDATETIME(),
            CONSTRAINT FK_FullDispatchCutoverBaseline_Cutover
                FOREIGN KEY (WarehouseID, CutoverID)
                REFERENCES dbo.FullDispatchCutovers (WarehouseID, CutoverID)
        );

        CREATE INDEX IX_FullDispatchCutoverBaseline_Lookup
            ON dbo.FullDispatchCutoverBaseline
               (WarehouseID, CutoverID, BN, ExpiryMonthKey, GenericItemNumber)
            INCLUDE (ClosedQuantityEach, ClosedQuantityPack, PackageSize);
    END;

    IF OBJECT_ID(N'dbo.FullDispatchTransactions', N'U') IS NULL
        THROW 52400, 'FullDispatchTransactions must exist before installing Full Dispatch cutover.', 1;

    IF COL_LENGTH(N'dbo.FullDispatchTransactions', N'CutoverID') IS NULL
        ALTER TABLE dbo.FullDispatchTransactions ADD CutoverID uniqueidentifier NULL;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
          AND name = N'IX_FullDispatchTransactions_CutoverBatch'
    )
    BEGIN
        CREATE INDEX IX_FullDispatchTransactions_CutoverBatch
            ON dbo.FullDispatchTransactions
               (WarehouseID, CutoverID, BN, ExpiryMonthKey, CreatedAt)
            INCLUDE
               (SubmittedQuantityPack, ConfirmedQuantityPack,
                SubmittedQuantityEach, ConfirmedQuantityEach);
    END;

    /* Add warehouse RLS to the new tables using the installed policy/function. */
    DECLARE
        @PolicyQualified nvarchar(517),
        @FunctionQualified nvarchar(517),
        @RlsSql nvarchar(max);

    SELECT TOP (1)
        @PolicyQualified = QUOTENAME(SCHEMA_NAME(sp.schema_id)) + N'.' + QUOTENAME(sp.name)
    FROM sys.security_policies sp
    WHERE sp.name = N'WarehouseSecurityPolicy';

    SELECT TOP (1)
        @FunctionQualified = QUOTENAME(SCHEMA_NAME(o.schema_id)) + N'.' + QUOTENAME(o.name)
    FROM sys.objects o
    WHERE o.name = N'fn_WarehouseAccessPredicate'
      AND o.type IN (N'IF', N'TF');

    IF @PolicyQualified IS NULL OR @FunctionQualified IS NULL
        THROW 52401, 'Warehouse RLS policy/function is unavailable for Full Dispatch cutover.', 1;

    IF NOT EXISTS
    (
        SELECT 1 FROM sys.security_predicates
        WHERE target_object_id = OBJECT_ID(N'dbo.FullDispatchCutovers')
          AND predicate_type_desc = N'FILTER'
    )
    BEGIN
        SET @RlsSql = N'ALTER SECURITY POLICY ' + @PolicyQualified
            + N' ADD FILTER PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutovers,'
            + N' ADD BLOCK PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutovers AFTER INSERT,'
            + N' ADD BLOCK PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutovers AFTER UPDATE;';
        EXEC sys.sp_executesql @RlsSql;
    END;

    IF NOT EXISTS
    (
        SELECT 1 FROM sys.security_predicates
        WHERE target_object_id = OBJECT_ID(N'dbo.FullDispatchCutoverBaseline')
          AND predicate_type_desc = N'FILTER'
    )
    BEGIN
        SET @RlsSql = N'ALTER SECURITY POLICY ' + @PolicyQualified
            + N' ADD FILTER PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutoverBaseline,'
            + N' ADD BLOCK PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutoverBaseline AFTER INSERT,'
            + N' ADD BLOCK PREDICATE ' + @FunctionQualified
            + N'(WarehouseID) ON dbo.FullDispatchCutoverBaseline AFTER UPDATE;';
        EXEC sys.sp_executesql @RlsSql;
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
