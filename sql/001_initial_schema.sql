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
       Latest Product Intelligence snapshots
       ================================================================ */
    IF OBJECT_ID(N'dbo.LatestInventorySnapshot', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.LatestInventorySnapshot
        (
            Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            BN nvarchar(255) NULL,
            ExpiryMonthKey nvarchar(20) NULL,
            ExpiryDate date NULL,
            GenericItemNumber nvarchar(255) NULL,
            TradeName nvarchar(500) NULL,
            AvailableQuantity decimal(20,4) NOT NULL DEFAULT (0),
            SourceFileName nvarchar(500) NULL,
            SnapshotUtc datetime2(0) NOT NULL DEFAULT (SYSUTCDATETIME())
        );
    END;

    IF COL_LENGTH(N'dbo.LatestInventorySnapshot', N'SourceFileName') IS NULL
        ALTER TABLE dbo.LatestInventorySnapshot ADD SourceFileName nvarchar(500) NULL;
    IF COL_LENGTH(N'dbo.LatestInventorySnapshot', N'SnapshotUtc') IS NULL
        ALTER TABLE dbo.LatestInventorySnapshot ADD SnapshotUtc datetime2(0) NOT NULL
            CONSTRAINT DF_LatestInventorySnapshot_SnapshotUtc_Upgrade DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    IF OBJECT_ID(N'dbo.LatestSFDASnapshot', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.LatestSFDASnapshot
        (
            Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            GTIN nvarchar(255) NULL,
            DrugName nvarchar(500) NULL,
            BN nvarchar(255) NULL,
            ExpiryMonthKey nvarchar(20) NULL,
            ExpiryDate date NULL,
            Quantity decimal(20,4) NOT NULL DEFAULT (0),
            Active decimal(20,4) NOT NULL DEFAULT (0),
            QuantitySentPending decimal(20,4) NOT NULL DEFAULT (0),
            QuantityReceivePending decimal(20,4) NOT NULL DEFAULT (0),
            SourceFileName nvarchar(500) NULL,
            SnapshotUtc datetime2(0) NOT NULL DEFAULT (SYSUTCDATETIME())
        );
    END;

    IF COL_LENGTH(N'dbo.LatestSFDASnapshot', N'SourceFileName') IS NULL
        ALTER TABLE dbo.LatestSFDASnapshot ADD SourceFileName nvarchar(500) NULL;
    IF COL_LENGTH(N'dbo.LatestSFDASnapshot', N'SnapshotUtc') IS NULL
        ALTER TABLE dbo.LatestSFDASnapshot ADD SnapshotUtc datetime2(0) NOT NULL
            CONSTRAINT DF_LatestSFDASnapshot_SnapshotUtc_Upgrade DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    /* ================================================================
       ReconciliationRuns and ReconciliationRunFiles
       Daily Upload & Run audit trail and downloadable-file registry.
       ================================================================ */
    IF OBJECT_ID(N'dbo.ReconciliationRuns', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.ReconciliationRuns
        (
            RunID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            RunNumber nvarchar(100) NOT NULL,
            ProcessType nvarchar(30) NOT NULL,
            Status nvarchar(50) NOT NULL,
            StartedAt datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME()),
            CompletedAt datetime2(3) NULL,
            SubmittedBy nvarchar(250) NULL,
            ASNFiles int NOT NULL DEFAULT (0),
            InventoryFiles int NOT NULL DEFAULT (0),
            DispatchFiles int NOT NULL DEFAULT (0),
            SFDAFiles int NOT NULL DEFAULT (0),
            TotalInputRows bigint NOT NULL DEFAULT (0),
            MasterRecords bigint NOT NULL DEFAULT (0),
            AcceptRecords bigint NOT NULL DEFAULT (0),
            DispatchRecords bigint NOT NULL DEFAULT (0),
            ExceptionRecords bigint NOT NULL DEFAULT (0),
            GeneratedFiles int NOT NULL DEFAULT (0),
            ApplicationVersion nvarchar(50) NULL,
            ErrorMessage nvarchar(max) NULL,
            CONSTRAINT UQ_ReconciliationRuns_RunNumber UNIQUE (RunNumber)
        );
    END;

    /* Safely upgrade an existing ReconciliationRuns table created by an older version. */
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'RunNumber') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD RunNumber nvarchar(100) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'ProcessType') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD ProcessType nvarchar(30) NOT NULL
            CONSTRAINT DF_ReconciliationRuns_ProcessType_Upgrade DEFAULT (N'UNKNOWN') WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'Status') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD Status nvarchar(50) NOT NULL
            CONSTRAINT DF_ReconciliationRuns_Status_Upgrade DEFAULT (N'Running') WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'StartedAt') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD StartedAt datetime2(3) NOT NULL
            CONSTRAINT DF_ReconciliationRuns_StartedAt_Upgrade DEFAULT (SYSUTCDATETIME()) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'CompletedAt') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD CompletedAt datetime2(3) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'SubmittedBy') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD SubmittedBy nvarchar(250) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'ASNFiles') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD ASNFiles int NOT NULL
            CONSTRAINT DF_ReconciliationRuns_ASNFiles_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'InventoryFiles') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD InventoryFiles int NOT NULL
            CONSTRAINT DF_ReconciliationRuns_InventoryFiles_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'DispatchFiles') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD DispatchFiles int NOT NULL
            CONSTRAINT DF_ReconciliationRuns_DispatchFiles_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'SFDAFiles') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD SFDAFiles int NOT NULL
            CONSTRAINT DF_ReconciliationRuns_SFDAFiles_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'TotalInputRows') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD TotalInputRows bigint NOT NULL
            CONSTRAINT DF_ReconciliationRuns_TotalInputRows_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'MasterRecords') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD MasterRecords bigint NOT NULL
            CONSTRAINT DF_ReconciliationRuns_MasterRecords_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'AcceptRecords') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD AcceptRecords bigint NOT NULL
            CONSTRAINT DF_ReconciliationRuns_AcceptRecords_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'DispatchRecords') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD DispatchRecords bigint NOT NULL
            CONSTRAINT DF_ReconciliationRuns_DispatchRecords_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'ExceptionRecords') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD ExceptionRecords bigint NOT NULL
            CONSTRAINT DF_ReconciliationRuns_ExceptionRecords_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'GeneratedFiles') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD GeneratedFiles int NOT NULL
            CONSTRAINT DF_ReconciliationRuns_GeneratedFiles_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'ApplicationVersion') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD ApplicationVersion nvarchar(50) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRuns', N'ErrorMessage') IS NULL
        ALTER TABLE dbo.ReconciliationRuns ADD ErrorMessage nvarchar(max) NULL;

    IF OBJECT_ID(N'dbo.ReconciliationRunFiles', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.ReconciliationRunFiles
        (
            RunFileID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
            RunID bigint NOT NULL,
            RunNumber nvarchar(100) NOT NULL,
            FileCategory nvarchar(30) NOT NULL,
            FileName nvarchar(500) NOT NULL,
            FileType nvarchar(30) NULL,
            ContainerName nvarchar(150) NULL,
            BlobName nvarchar(1000) NULL,
            ContentType nvarchar(250) NULL,
            SizeBytes bigint NOT NULL DEFAULT (0),
            ETag nvarchar(250) NULL,
            CreatedAt datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT FK_ReconciliationRunFiles_Run
                FOREIGN KEY (RunID) REFERENCES dbo.ReconciliationRuns(RunID)
        );
    END;

    /* Safely upgrade an existing ReconciliationRunFiles table. */
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'RunID') IS NULL
    BEGIN
        ALTER TABLE dbo.ReconciliationRunFiles ADD RunID bigint NULL;

        UPDATE f
        SET RunID = r.RunID
        FROM dbo.ReconciliationRunFiles AS f
        INNER JOIN dbo.ReconciliationRuns AS r
            ON r.RunNumber = f.RunNumber
        WHERE f.RunID IS NULL;

        IF NOT EXISTS
        (
            SELECT 1 FROM dbo.ReconciliationRunFiles WHERE RunID IS NULL
        )
            ALTER TABLE dbo.ReconciliationRunFiles ALTER COLUMN RunID bigint NOT NULL;
    END;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'RunNumber') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD RunNumber nvarchar(100) NULL;

    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'RunID') IS NOT NULL
    BEGIN
        UPDATE f
        SET RunID = r.RunID
        FROM dbo.ReconciliationRunFiles AS f
        INNER JOIN dbo.ReconciliationRuns AS r
            ON r.RunNumber = f.RunNumber
        WHERE f.RunID IS NULL;
    END;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'FileCategory') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD FileCategory nvarchar(30) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'FileName') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD FileName nvarchar(500) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'FileType') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD FileType nvarchar(30) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'ContainerName') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD ContainerName nvarchar(150) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'BlobName') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD BlobName nvarchar(1000) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'ContentType') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD ContentType nvarchar(250) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'SizeBytes') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD SizeBytes bigint NOT NULL
            CONSTRAINT DF_ReconciliationRunFiles_SizeBytes_Upgrade DEFAULT (0) WITH VALUES;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'ETag') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD ETag nvarchar(250) NULL;
    IF COL_LENGTH(N'dbo.ReconciliationRunFiles', N'CreatedAt') IS NULL
        ALTER TABLE dbo.ReconciliationRunFiles ADD CreatedAt datetime2(3) NOT NULL
            CONSTRAINT DF_ReconciliationRunFiles_CreatedAt_Upgrade DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.foreign_keys
        WHERE name = N'FK_ReconciliationRunFiles_Run'
          AND parent_object_id = OBJECT_ID(N'dbo.ReconciliationRunFiles')
    )
    AND NOT EXISTS
    (
        SELECT 1 FROM dbo.ReconciliationRunFiles WHERE RunID IS NULL
    )
    BEGIN
        ALTER TABLE dbo.ReconciliationRunFiles
        ADD CONSTRAINT FK_ReconciliationRunFiles_Run
            FOREIGN KEY (RunID) REFERENCES dbo.ReconciliationRuns(RunID);
    END;

    IF OBJECT_ID(N'dbo.DailyProcessedTransactions', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyProcessedTransactions
        (
            TransactionKey varchar(64) NOT NULL PRIMARY KEY,
            ProcessType nvarchar(30) NOT NULL,
            PayloadJson nvarchar(max) NOT NULL,
            CreatedAt datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME())
        );
    END;

    /* Safely upgrade an existing DailyProcessedTransactions table.
       Older deployments used TransactionType. Keep that column intact and
       synchronize it with ProcessType so historical de-duplication remains
       compatible without deleting any prior daily transactions. */
    IF COL_LENGTH(N'dbo.DailyProcessedTransactions', N'ProcessType') IS NULL
        ALTER TABLE dbo.DailyProcessedTransactions ADD ProcessType nvarchar(30) NULL;

    IF COL_LENGTH(N'dbo.DailyProcessedTransactions', N'TransactionType') IS NOT NULL
    BEGIN
        UPDATE dbo.DailyProcessedTransactions
        SET ProcessType = NULLIF(LTRIM(RTRIM(TransactionType)), N'')
        WHERE (ProcessType IS NULL OR LTRIM(RTRIM(ProcessType)) IN (N'', N'UNKNOWN'))
          AND NULLIF(LTRIM(RTRIM(TransactionType)), N'') IS NOT NULL;
    END;

    UPDATE dbo.DailyProcessedTransactions
    SET ProcessType = N'UNKNOWN'
    WHERE ProcessType IS NULL OR LTRIM(RTRIM(ProcessType)) = N'';

    IF EXISTS
    (
        SELECT 1
        FROM sys.columns
        WHERE object_id = OBJECT_ID(N'dbo.DailyProcessedTransactions')
          AND name = N'ProcessType'
          AND is_nullable = 1
    )
        ALTER TABLE dbo.DailyProcessedTransactions ALTER COLUMN ProcessType nvarchar(30) NOT NULL;

    IF COL_LENGTH(N'dbo.DailyProcessedTransactions', N'PayloadJson') IS NULL
        ALTER TABLE dbo.DailyProcessedTransactions ADD PayloadJson nvarchar(max) NULL;
    IF COL_LENGTH(N'dbo.DailyProcessedTransactions', N'CreatedAt') IS NULL
        ALTER TABLE dbo.DailyProcessedTransactions ADD CreatedAt datetime2(3) NOT NULL
            CONSTRAINT DF_DailyProcessedTransactions_CreatedAt_Upgrade DEFAULT (SYSUTCDATETIME()) WITH VALUES;

    /* ================================================================
       Daily Accept confirmation state
       ================================================================ */
    IF OBJECT_ID(N'dbo.DailyAcceptTransactions', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyAcceptTransactions
        (
            TransactionKey varchar(64) NOT NULL
                CONSTRAINT PK_DailyAcceptTransactions PRIMARY KEY,
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            ExpiryMonthKey nvarchar(7) NOT NULL,
            GenericItemNumber nvarchar(255) NULL,
            ReferenceNumber nvarchar(255) NULL,
            ReferenceLine nvarchar(255) NULL,
            SubmittedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_SubmittedEach DEFAULT (0),
            SubmittedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_SubmittedPack DEFAULT (0),
            ConfirmedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_ConfirmedEach DEFAULT (0),
            ConfirmedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_ConfirmedPack DEFAULT (0),
            FirstSubmittedRun nvarchar(80) NULL,
            LastSubmittedRun nvarchar(80) NULL,
            CreatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_CreatedAt DEFAULT (SYSUTCDATETIME()),
            UpdatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_DailyAcceptTransactions_UpdatedAt DEFAULT (SYSUTCDATETIME()),
            LastConfirmedAt datetime2(3) NULL
        );
    END;

    IF OBJECT_ID(N'dbo.DailyAcceptSFDABaseline', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyAcceptSFDABaseline
        (
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            GTIN nvarchar(255) NULL,
            Active decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptSFDABaseline_Active DEFAULT (0),
            QuantityReceivePending decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyAcceptSFDABaseline_Pending DEFAULT (0),
            SourceFileName nvarchar(500) NULL,
            SnapshotUtc datetime2(3) NOT NULL
                CONSTRAINT DF_DailyAcceptSFDABaseline_SnapshotUtc DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_DailyAcceptSFDABaseline PRIMARY KEY (BN, ExpiryDate)
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.DailyAcceptTransactions')
          AND name = N'IX_DailyAcceptTransactions_BatchPending'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_DailyAcceptTransactions_BatchPending
        ON dbo.DailyAcceptTransactions (BN, ExpiryDate, CreatedAt)
        INCLUDE
        (
            SubmittedQuantityEach, ConfirmedQuantityEach,
            SubmittedQuantityPack, ConfirmedQuantityPack, LastConfirmedAt
        );
    END;

    /* ================================================================
       Daily Dispatch confirmation state
       ================================================================ */
    IF OBJECT_ID(N'dbo.DailyDispatchTransactions', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyDispatchTransactions
        (
            TransactionKey varchar(64) NOT NULL
                CONSTRAINT PK_DailyDispatchTransactions PRIMARY KEY,
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            GenericItemNumber nvarchar(255) NULL,
            ReferenceNumber nvarchar(255) NULL,
            ReferenceLine nvarchar(255) NULL,
            ToAddress nvarchar(500) NULL,
            TransactionDate datetime2 NULL,
            SubmittedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_SubmittedEach DEFAULT (0),
            SubmittedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_SubmittedPack DEFAULT (0),
            ConfirmedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_ConfirmedEach DEFAULT (0),
            ConfirmedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_ConfirmedPack DEFAULT (0),
            FirstSubmittedRun nvarchar(80) NULL,
            LastSubmittedRun nvarchar(80) NULL,
            CreatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_CreatedAt DEFAULT (SYSUTCDATETIME()),
            UpdatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_DailyDispatchTransactions_UpdatedAt DEFAULT (SYSUTCDATETIME()),
            LastConfirmedAt datetime2(3) NULL
        );
    END;

    IF OBJECT_ID(N'dbo.DailyDispatchSFDABaseline', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyDispatchSFDABaseline
        (
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            GTIN nvarchar(255) NULL,
            Active decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchSFDABaseline_Active DEFAULT (0),
            QuantitySentPending decimal(18,4) NOT NULL
                CONSTRAINT DF_DailyDispatchSFDABaseline_SentPending DEFAULT (0),
            SourceFileName nvarchar(500) NULL,
            SnapshotUtc datetime2(3) NOT NULL
                CONSTRAINT DF_DailyDispatchSFDABaseline_SnapshotUtc DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_DailyDispatchSFDABaseline PRIMARY KEY (BN, ExpiryDate)
        );
    END;

    IF OBJECT_ID(N'dbo.DailyDispatchConfirmations', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.DailyDispatchConfirmations
        (
            ConfirmationKey varchar(64) NOT NULL
                CONSTRAINT PK_DailyDispatchConfirmations PRIMARY KEY,
            TransactionKey varchar(64) NOT NULL,
            ConfirmedQuantityEach decimal(18,4) NOT NULL,
            ConfirmedQuantityPack decimal(18,4) NOT NULL,
            ConfirmedAt datetime2(3) NOT NULL
                CONSTRAINT DF_DailyDispatchConfirmations_ConfirmedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT FK_DailyDispatchConfirmations_Transaction
                FOREIGN KEY (TransactionKey)
                REFERENCES dbo.DailyDispatchTransactions(TransactionKey)
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.DailyDispatchTransactions')
          AND name = N'IX_DailyDispatchTransactions_BatchPending'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_DailyDispatchTransactions_BatchPending
        ON dbo.DailyDispatchTransactions (BN, ExpiryDate, CreatedAt)
        INCLUDE
        (
            SubmittedQuantityEach, ConfirmedQuantityEach,
            SubmittedQuantityPack, ConfirmedQuantityPack, LastConfirmedAt
        );
    END;


    /* ================================================================
       Full Dispatch confirmation state
       ================================================================ */
    IF OBJECT_ID(N'dbo.FullDispatchTransactions', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FullDispatchTransactions
        (
            TransactionKey varchar(64) NOT NULL
                CONSTRAINT PK_FullDispatchTransactions PRIMARY KEY,
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            ExpiryMonthKey nvarchar(7) NOT NULL,
            GenericItemNumber nvarchar(255) NULL,
            ToAddress nvarchar(500) NULL,
            GLN nvarchar(255) NULL,
            SubmittedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_SubmittedEach DEFAULT (0),
            SubmittedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_SubmittedPack DEFAULT (0),
            ConfirmedQuantityEach decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_ConfirmedEach DEFAULT (0),
            ConfirmedQuantityPack decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_ConfirmedPack DEFAULT (0),
            FirstSubmittedRun nvarchar(80) NULL,
            LastSubmittedRun nvarchar(80) NULL,
            CreatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_CreatedAt DEFAULT (SYSUTCDATETIME()),
            UpdatedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchTransactions_UpdatedAt DEFAULT (SYSUTCDATETIME()),
            LastConfirmedAt datetime2(3) NULL
        );
    END;

    IF COL_LENGTH(N'dbo.FullDispatchTransactions', N'ExpiryMonthKey') IS NULL
    BEGIN
        ALTER TABLE dbo.FullDispatchTransactions
            ADD ExpiryMonthKey nvarchar(7) NULL;

        UPDATE dbo.FullDispatchTransactions
        SET ExpiryMonthKey = CONVERT(char(7), ExpiryDate, 126)
        WHERE ExpiryMonthKey IS NULL;

        ALTER TABLE dbo.FullDispatchTransactions
            ALTER COLUMN ExpiryMonthKey nvarchar(7) NOT NULL;
    END;

    IF OBJECT_ID(N'dbo.FullDispatchSFDABaseline', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FullDispatchSFDABaseline
        (
            BN nvarchar(255) NOT NULL,
            ExpiryDate date NOT NULL,
            GTIN nvarchar(255) NULL,
            Active decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchSFDABaseline_Active DEFAULT (0),
            QuantitySentPending decimal(18,4) NOT NULL
                CONSTRAINT DF_FullDispatchSFDABaseline_SentPending DEFAULT (0),
            SourceFileName nvarchar(500) NULL,
            SnapshotUtc datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchSFDABaseline_SnapshotUtc DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_FullDispatchSFDABaseline PRIMARY KEY (BN, ExpiryDate)
        );
    END;

    IF OBJECT_ID(N'dbo.FullDispatchConfirmations', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FullDispatchConfirmations
        (
            ConfirmationKey varchar(64) NOT NULL
                CONSTRAINT PK_FullDispatchConfirmations PRIMARY KEY,
            TransactionKey varchar(64) NOT NULL,
            ConfirmedQuantityEach decimal(18,4) NOT NULL,
            ConfirmedQuantityPack decimal(18,4) NOT NULL,
            ConfirmedAt datetime2(3) NOT NULL
                CONSTRAINT DF_FullDispatchConfirmations_ConfirmedAt DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT FK_FullDispatchConfirmations_Transaction
                FOREIGN KEY (TransactionKey)
                REFERENCES dbo.FullDispatchTransactions(TransactionKey)
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'dbo.FullDispatchTransactions')
          AND name = N'IX_FullDispatchTransactions_BatchPending'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX IX_FullDispatchTransactions_BatchPending
        ON dbo.FullDispatchTransactions (BN, ExpiryMonthKey, CreatedAt)
        INCLUDE
        (
            ToAddress, GLN,
            SubmittedQuantityEach, ConfirmedQuantityEach,
            SubmittedQuantityPack, ConfirmedQuantityPack, LastConfirmedAt
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



    /* ================================================================
       HistoricalBuildJobs
       Background job state for long-running historical data builds.
       ================================================================ */
    IF OBJECT_ID(N'dbo.HistoricalBuildJobs', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.HistoricalBuildJobs
        (
            JobID               nvarchar(64)     NOT NULL,
            Operation           nvarchar(20)     NOT NULL,
            Status              nvarchar(30)     NOT NULL,
            Progress            int              NOT NULL
                CONSTRAINT DF_HistoricalBuildJobs_Progress DEFAULT (0),
            CurrentStage        nvarchar(250)    NULL,
            InputManifestJson   nvarchar(max)    NULL,
            OutputManifestJson  nvarchar(max)    NULL,
            SummaryJson         nvarchar(max)    NULL,
            ErrorMessage        nvarchar(max)    NULL,
            CreatedAt           datetime2(0)     NOT NULL
                CONSTRAINT DF_HistoricalBuildJobs_CreatedAt DEFAULT (SYSUTCDATETIME()),
            StartedAt           datetime2(0)     NULL,
            CompletedAt         datetime2(0)     NULL,
            UpdatedAt           datetime2(0)     NOT NULL
                CONSTRAINT DF_HistoricalBuildJobs_UpdatedAt DEFAULT (SYSUTCDATETIME()),

            CONSTRAINT PK_HistoricalBuildJobs PRIMARY KEY (JobID)
        );
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.indexes
        WHERE name = N'IX_HistoricalBuildJobs_StatusCreatedAt'
          AND object_id = OBJECT_ID(N'dbo.HistoricalBuildJobs')
    )
    BEGIN
        CREATE INDEX IX_HistoricalBuildJobs_StatusCreatedAt
            ON dbo.HistoricalBuildJobs (Status, CreatedAt DESC);
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
