/*
    Customer-code bridge for Return / Cancel Dispatch

    Full Dispatch "Ship To Customer" is persisted as CustomerCode and matched
    to ASN "Supplier Code" inside the same warehouse and regulatory batch key.
    Existing quantities are not changed. Replaying a historical Full Dispatch
    file enriches duplicate EventKeys in place and the normal affected-key
    refresh rebuilds CustomerHistory without duplicating movements.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    EXEC sys.sp_set_session_context @key=N'WarehouseID', @value=NULL;

    IF OBJECT_ID(N'dbo.DispatchEvents', N'U') IS NULL
        THROW 52500, 'DispatchEvents must exist before installing Customer Code matching.', 1;

    IF OBJECT_ID(N'dbo.CustomerHistory', N'U') IS NULL
        THROW 52501, 'CustomerHistory must exist before installing Customer Code matching.', 1;

    IF COL_LENGTH(N'dbo.DispatchEvents', N'CustomerCode') IS NULL
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.DispatchEvents ADD CustomerCode nvarchar(255) NULL;
        ';

    IF COL_LENGTH(N'dbo.CustomerHistory', N'CustomerCode') IS NULL
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.CustomerHistory ADD CustomerCode nvarchar(255) NULL;
        ';

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.DispatchEvents')
          AND name = N'IX_DispatchEvents_CustomerCodeBatch'
    )
    BEGIN
        EXEC sys.sp_executesql N'
            CREATE INDEX IX_DispatchEvents_CustomerCodeBatch
                ON dbo.DispatchEvents
                   (CustomerCode, BN, ExpiryMonthKey, GenericItemNumber)
                INCLUDE (ToAddress, DispatchedQuantity, DispatchDate, TradeItemNumber);
        ';
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.CustomerHistory')
          AND name = N'IX_CustomerHistory_CustomerCodeBatch'
    )
    BEGIN
        EXEC sys.sp_executesql N'
            CREATE INDEX IX_CustomerHistory_CustomerCodeBatch
                ON dbo.CustomerHistory
                   (CustomerCode, BN, ExpiryMonthKey, GenericItemNumber)
                INCLUDE (ToAddress, GLN, DispatchQuantityEach, DispatchQuantityPack);
        ';
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
