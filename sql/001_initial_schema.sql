/*
    SFDA Reconciliation v5.0
    Idempotent Azure SQL schema

    This script may be executed repeatedly.
    It creates missing Version 5 tables, adds missing columns,
    defaults and performance indexes without deleting existing data.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    /* ================================================================
       ReceiptEvents
       ================================================================ */
    IF OBJECT_ID(N'dbo.ReceiptEvents', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.ReceiptEvents
        (
            EventKey             varchar(64)      NOT NULL,
            BN                   nvarchar(120)    NOT NULL,
            ExpiryMonthKey       char(7)          NOT NULL,
            ExpiryDate           date             NULL,
            GenericItemNumber    nvarchar(120)    NOT NULL,
            TradeItemNumber      nvarchar(120)    NULL,
            TradeName            nvarchar(500)    NULL,
            ReceivedQuantity     decimal(20, 4)   NOT NULL
                CONSTRAINT DF_ReceiptEvents_ReceivedQuantity DEFAULT (0),
            InboundShipment      nvarchar(150)    NULL,
            ASNLine              nvarchar(100)    NULL,
            SupplierName         nvarchar(500)    NULL,
            SupplierCode         nvarchar(120)    NULL,
            Description          nvarchar(1000)   NULL,
            ItemFamilyGroup      nvarchar(500)    NULL,
            ReceivedDate         datetime2(3)     NULL,
            CreatedAt            datetime2(3)     NOT NULL
                CONSTRAINT DF_ReceiptEvents_CreatedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_ReceiptEvents PRIMARY KEY CLUSTERED (EventKey)
        );
    END;

    IF COL_LENGTH(N'dbo.ReceiptEvents', N'SupplierCode') IS NULL
        ALTER TABLE dbo.ReceiptEvents ADD SupplierCode nvarchar(120) NULL;

    IF COL_LENGTH(N'dbo.ReceiptEvents', N'Description') IS NULL
        ALTER TABLE dbo.ReceiptEvents ADD Description nvarchar(1000) NULL;

    IF COL_LENGTH(N'dbo.ReceiptEvents', N'ItemFamilyGroup') IS NULL
        ALTER TABLE dbo.ReceiptEvents ADD ItemFamilyGroup nvarchar(500) NULL;

    IF COL_LENGTH(N'dbo.ReceiptEvents', N'CreatedAt') IS NULL
        ALTER TABLE dbo.ReceiptEvents
        ADD CreatedAt datetime2(3) NOT NULL
            CONSTRAINT DF_ReceiptEvents_CreatedAt_Migration
            DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    /* ================================================================
       DispatchEvents
       ================================================================ */
    IF OBJECT_ID(N'dbo.DispatchEvents', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DispatchEvents
        (
            EventKey             varchar(64)      NOT NULL,
            BN                   nvarchar(120)    NOT NULL,
            ExpiryMonthKey       char(7)          NOT NULL,
            ExpiryDate           date             NULL,
            GenericItemNumber    nvarchar(120)    NOT NULL,
            TradeItemNumber      nvarchar(120)    NULL,
            TradeName            nvarchar(500)    NULL,
            DispatchedQuantity   decimal(20, 4)   NOT NULL
                CONSTRAINT DF_DispatchEvents_DispatchedQuantity DEFAULT (0),
            ToAddress            nvarchar(500)    NULL,
            SalesOrderNumber     nvarchar(150)    NULL,
            OrderLine            nvarchar(100)    NULL,
            DispatchDate         datetime2(3)     NULL,
            CreatedAt            datetime2(3)     NOT NULL
                CONSTRAINT DF_DispatchEvents_CreatedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_DispatchEvents PRIMARY KEY CLUSTERED (EventKey)
        );
    END;

    IF COL_LENGTH(N'dbo.DispatchEvents', N'CreatedAt') IS NULL
        ALTER TABLE dbo.DispatchEvents
        ADD CreatedAt datetime2(3) NOT NULL
            CONSTRAINT DF_DispatchEvents_CreatedAt_Migration
            DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    /* ================================================================
       BatchMaster
       ================================================================ */
    IF OBJECT_ID(N'dbo.BatchMaster', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.BatchMaster
        (
            BN                       nvarchar(120)   NOT NULL,
            ExpiryMonthKey           char(7)         NOT NULL,
            ExpiryDate               date            NULL,
            GenericItemNumber        nvarchar(120)   NOT NULL,
            TradeItemNumber          nvarchar(120)   NULL,
            TradeName                nvarchar(500)   NULL,
            GTIN                     nvarchar(20)    NULL,
            DrugName                 nvarchar(500)   NULL,
            PackageSize              decimal(20, 4)  NULL,
            SFDAQuantity             decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_SFDAQuantity DEFAULT (0),
            Active                   decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_Active DEFAULT (0),
            QuantitySentPending      decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_QuantitySentPending DEFAULT (0),
            QuantityReceivePending   decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_QuantityReceivePending DEFAULT (0),
            Description              nvarchar(1000)  NULL,
            ItemFamilyGroup          nvarchar(500)   NULL,
            SupplierName             nvarchar(500)   NULL,
            SupplierCode             nvarchar(120)   NULL,
            TotalReceiveQty          decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_TotalReceiveQty DEFAULT (0),
            TotalDispatchedQty       decimal(20, 4)  NOT NULL
                CONSTRAINT DF_BatchMaster_TotalDispatchedQty DEFAULT (0),
            ReceiveRuns              int             NOT NULL
                CONSTRAINT DF_BatchMaster_ReceiveRuns DEFAULT (0),
            DispatchRuns             int             NOT NULL
                CONSTRAINT DF_BatchMaster_DispatchRuns DEFAULT (0),
            FirstReceivedDate        datetime2(3)    NULL,
            LastReceivedDate         datetime2(3)    NULL,
            FirstDispatchDate        datetime2(3)    NULL,
            LastDispatchDate         datetime2(3)    NULL,
            GenericExistsInSFDA      nvarchar(30)    NOT NULL
                CONSTRAINT DF_BatchMaster_GenericExistsInSFDA DEFAULT (N'Yes'),
            LastUpdated              datetime2(3)    NOT NULL
                CONSTRAINT DF_BatchMaster_LastUpdated DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_BatchMaster PRIMARY KEY CLUSTERED
            (
                BN,
                ExpiryMonthKey,
                GenericItemNumber
            )
        );
    END;

    IF COL_LENGTH(N'dbo.BatchMaster', N'PackageSize') IS NULL
        ALTER TABLE dbo.BatchMaster ADD PackageSize decimal(20, 4) NULL;

    IF COL_LENGTH(N'dbo.BatchMaster', N'SFDAQuantity') IS NULL
        ALTER TABLE dbo.BatchMaster
        ADD SFDAQuantity decimal(20, 4) NOT NULL
            CONSTRAINT DF_BatchMaster_SFDAQuantity_Migration DEFAULT (0) WITH VALUES;

    IF COL_LENGTH(N'dbo.BatchMaster', N'Active') IS NULL
        ALTER TABLE dbo.BatchMaster
        ADD Active decimal(20, 4) NOT NULL
            CONSTRAINT DF_BatchMaster_Active_Migration DEFAULT (0) WITH VALUES;

    IF COL_LENGTH(N'dbo.BatchMaster', N'QuantitySentPending') IS NULL
        ALTER TABLE dbo.BatchMaster
        ADD QuantitySentPending decimal(20, 4) NOT NULL
            CONSTRAINT DF_BatchMaster_QuantitySentPending_Migration DEFAULT (0) WITH VALUES;

    IF COL_LENGTH(N'dbo.BatchMaster', N'QuantityReceivePending') IS NULL
        ALTER TABLE dbo.BatchMaster
        ADD QuantityReceivePending decimal(20, 4) NOT NULL
            CONSTRAINT DF_BatchMaster_QuantityReceivePending_Migration DEFAULT (0) WITH VALUES;

    IF COL_LENGTH(N'dbo.BatchMaster', N'Description') IS NULL
        ALTER TABLE dbo.BatchMaster ADD Description nvarchar(1000) NULL;

    IF COL_LENGTH(N'dbo.BatchMaster', N'ItemFamilyGroup') IS NULL
        ALTER TABLE dbo.BatchMaster ADD ItemFamilyGroup nvarchar(500) NULL;

    IF COL_LENGTH(N'dbo.BatchMaster', N'SupplierName') IS NULL
        ALTER TABLE dbo.BatchMaster ADD SupplierName nvarchar(500) NULL;

    IF COL_LENGTH(N'dbo.BatchMaster', N'SupplierCode') IS NULL
        ALTER TABLE dbo.BatchMaster ADD SupplierCode nvarchar(120) NULL;

    /* ================================================================
       SupplierHistory and CustomerHistory
       ================================================================ */
    IF OBJECT_ID(N'dbo.SupplierHistory', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.SupplierHistory
        (
            SupplierHistoryID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            SupplierName nvarchar(500) NULL,
            SupplierCode nvarchar(120) NULL,
            GTIN nvarchar(20) NULL,
            DrugName nvarchar(500) NULL,
            GenericItemNumber nvarchar(120) NOT NULL,
            Description nvarchar(1000) NULL,
            TradeDescription nvarchar(500) NULL,
            BN nvarchar(120) NOT NULL,
            ExpiryMonthKey char(7) NOT NULL,
            ExpiryDate date NULL,
            PackageSize decimal(20,4) NULL,
            ReceivedQuantityEach decimal(20,4) NOT NULL DEFAULT (0),
            ReceivedQuantityPack decimal(20,4) NOT NULL DEFAULT (0),
            FirstReceivedDate datetime2(3) NULL,
            LastReceivedDate datetime2(3) NULL,
            ItemFamilyGroup nvarchar(500) NULL,
            TradeItemNumber nvarchar(120) NULL,
            LastUpdated datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME())
        );
    END;

    IF OBJECT_ID(N'dbo.CustomerHistory', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.CustomerHistory
        (
            CustomerHistoryID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ToAddress nvarchar(500) NULL,
            GLN nvarchar(30) NULL,
            GTIN nvarchar(20) NULL,
            DrugName nvarchar(500) NULL,
            GenericItemNumber nvarchar(120) NOT NULL,
            TradeDescription nvarchar(500) NULL,
            BN nvarchar(120) NOT NULL,
            ExpiryMonthKey char(7) NOT NULL,
            ExpiryDate date NULL,
            PackageSize decimal(20,4) NULL,
            DispatchQuantityEach decimal(20,4) NOT NULL DEFAULT (0),
            DispatchQuantityPack decimal(20,4) NOT NULL DEFAULT (0),
            FirstDispatchDate datetime2(3) NULL,
            LastDispatchDate datetime2(3) NULL,
            TradeItemNumber nvarchar(120) NULL,
            LastUpdated datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME())
        );
    END;

    /* ================================================================
       RunHistory
       ================================================================ */
    IF OBJECT_ID(N'dbo.RunHistory', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.RunHistory
        (
            RunID           uniqueidentifier NOT NULL,
            RunType         nvarchar(100)    NOT NULL,
            Status          nvarchar(50)     NOT NULL,
            StartedAt       datetime2(3)     NOT NULL,
            CompletedAt     datetime2(3)     NULL,
            SummaryJson     nvarchar(max)    NULL,
            ErrorMessage    nvarchar(max)    NULL,
            CreatedAt       datetime2(3)     NOT NULL
                CONSTRAINT DF_RunHistory_CreatedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_RunHistory PRIMARY KEY CLUSTERED (RunID)
        );
    END;

    /* ================================================================
       Performance indexes
       ================================================================ */
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.ReceiptEvents')
          AND name = N'IX_ReceiptEvents_BatchSummary'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_ReceiptEvents_BatchSummary
        ON dbo.ReceiptEvents
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber
        )
        INCLUDE
        (
            ReceivedQuantity,
            ReceivedDate,
            TradeItemNumber,
            TradeName,
            SupplierName,
            SupplierCode,
            Description,
            ItemFamilyGroup
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.DispatchEvents')
          AND name = N'IX_DispatchEvents_BatchSummary'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_DispatchEvents_BatchSummary
        ON dbo.DispatchEvents
        (
            BN,
            ExpiryMonthKey,
            GenericItemNumber
        )
        INCLUDE
        (
            DispatchedQuantity,
            DispatchDate,
            TradeItemNumber,
            TradeName,
            ToAddress,
            SalesOrderNumber,
            OrderLine
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.DispatchEvents')
          AND name = N'IX_DispatchEvents_DispatchEvidence'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_DispatchEvents_DispatchEvidence
        ON dbo.DispatchEvents
        (
            DispatchDate,
            SalesOrderNumber,
            OrderLine
        )
        INCLUDE
        (
            EventKey,
            BN,
            ExpiryMonthKey,
            ExpiryDate,
            GenericItemNumber,
            TradeItemNumber,
            TradeName,
            DispatchedQuantity,
            ToAddress
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.BatchMaster')
          AND name = N'IX_BatchMaster_GenericItemNumber'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_BatchMaster_GenericItemNumber
        ON dbo.BatchMaster (GenericItemNumber)
        INCLUDE
        (
            BN,
            ExpiryMonthKey,
            GTIN,
            DrugName,
            GenericExistsInSFDA,
            LastUpdated
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.RunHistory')
          AND name = N'IX_RunHistory_StartedAt'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_RunHistory_StartedAt
        ON dbo.RunHistory (StartedAt DESC, CreatedAt DESC)
        INCLUDE (RunType, Status, CompletedAt);
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.SupplierHistory') AND name=N'IX_SupplierHistory_Lookup')
        CREATE INDEX IX_SupplierHistory_Lookup ON dbo.SupplierHistory (SupplierCode, GenericItemNumber, BN, ExpiryMonthKey);

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.CustomerHistory') AND name=N'IX_CustomerHistory_Lookup')
        CREATE INDEX IX_CustomerHistory_Lookup ON dbo.CustomerHistory (GLN, GenericItemNumber, BN, ExpiryMonthKey);

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
