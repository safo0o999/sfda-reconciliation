import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pymssql


class Database:
    def __init__(self) -> None:
        self.connection_string = os.getenv(
            "SQL_CONNECTION_STRING"
        )

        if not self.connection_string:
            raise RuntimeError(
                "SQL_CONNECTION_STRING environment variable is missing."
            )

    def _parse_connection_string(
        self
    ) -> Dict[str, str]:

        parts: Dict[str, str] = {}

        for item in self.connection_string.split(";"):
            item = item.strip()

            if not item or "=" not in item:
                continue

            key, value = item.split("=", 1)

            parts[
                key.strip().lower()
            ] = value.strip()

        required_keys = [
            "server",
            "initial catalog",
            "user id",
            "password",
        ]

        missing_keys = [
            key
            for key in required_keys
            if not parts.get(key)
        ]

        if missing_keys:
            raise RuntimeError(
                "Missing SQL connection values: "
                + ", ".join(missing_keys)
            )

        return parts

    def connect(self):
        parts = self._parse_connection_string()

        server = (
            parts["server"]
            .replace("tcp:", "")
            .split(",")[0]
            .strip()
        )

        return pymssql.connect(
            server=server,
            user=parts["user id"],
            password=parts["password"],
            database=parts["initial catalog"],
            port=1433,
            login_timeout=30,
            timeout=120,
            charset="UTF-8",
        )

    def execute(
        self,
        sql: str,
        parameters: Optional[tuple] = None,
    ) -> None:

        connection = self.connect()

        try:
            cursor = connection.cursor()

            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.execute(
                    sql,
                    parameters
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()


    def execute_many(
        self,
        sql: str,
        parameters_list: List[tuple],
    ) -> None:

        if not parameters_list:
            return

        connection = self.connect()

        try:
            cursor = connection.cursor()
            cursor.executemany(sql, parameters_list)
            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()


    def execute_scalar(
        self,
        sql: str,
        parameters: Optional[tuple] = None,
    ) -> Any:

        connection = self.connect()

        try:
            cursor = connection.cursor()

            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.execute(
                    sql,
                    parameters
                )

            row = cursor.fetchone()

            connection.commit()

            if not row:
                return None

            return row[0]

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    def fetch_one(
        self,
        sql: str,
        parameters: Optional[tuple] = None,
    ) -> Optional[Dict[str, Any]]:

        connection = self.connect()

        try:
            cursor = connection.cursor(
                as_dict=True
            )

            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.execute(
                    sql,
                    parameters
                )

            return cursor.fetchone()

        finally:
            connection.close()

    def fetch_all(
        self,
        sql: str,
        parameters: Optional[tuple] = None,
    ) -> List[Dict[str, Any]]:

        connection = self.connect()

        try:
            cursor = connection.cursor(
                as_dict=True
            )

            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.execute(
                    sql,
                    parameters
                )

            return list(
                cursor.fetchall()
            )

        finally:
            connection.close()


def initialize_database() -> None:
    database = Database()

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.ReconciliationRuns',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.ReconciliationRuns
            (
                RunID BIGINT IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT PK_ReconciliationRuns
                    PRIMARY KEY,

                RunNumber NVARCHAR(50)
                    NOT NULL,

                RunDate DATETIME2(3)
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_RunDate
                    DEFAULT SYSUTCDATETIME(),

                SubmittedBy NVARCHAR(200)
                    NULL,

                ASNFiles INT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_ASNFiles
                    DEFAULT 0,

                DispatchFiles INT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_DispatchFiles
                    DEFAULT 0,

                InventoryFiles INT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_InventoryFiles
                    DEFAULT 0,

                SFDAFiles INT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_SFDAFiles
                    DEFAULT 0,

                TotalInputRows BIGINT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_TotalInputRows
                    DEFAULT 0,

                MasterRecords BIGINT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_MasterRecords
                    DEFAULT 0,

                AcceptRecords BIGINT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_AcceptRecords
                    DEFAULT 0,

                DispatchRecords BIGINT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_DispatchRecords
                    DEFAULT 0,

                ExceptionRecords BIGINT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_ExceptionRecords
                    DEFAULT 0,

                GeneratedFiles INT
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_GeneratedFiles
                    DEFAULT 0,

                ApplicationVersion NVARCHAR(30)
                    NULL,

                Status NVARCHAR(50)
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_Status
                    DEFAULT N'Pending',

                ErrorMessage NVARCHAR(MAX)
                    NULL,

                StartedAt DATETIME2(3)
                    NOT NULL
                    CONSTRAINT DF_ReconciliationRuns_StartedAt
                    DEFAULT SYSUTCDATETIME(),

                CompletedAt DATETIME2(3)
                    NULL,

                CONSTRAINT UQ_ReconciliationRuns_RunNumber
                    UNIQUE (RunNumber)
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE
                name =
                N'IX_ReconciliationRuns_RunDate'
                AND object_id =
                OBJECT_ID(
                    N'dbo.ReconciliationRuns'
                )
        )
        BEGIN
            CREATE INDEX
                IX_ReconciliationRuns_RunDate
            ON dbo.ReconciliationRuns
            (
                RunDate DESC
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE
                name =
                N'IX_ReconciliationRuns_Status'
                AND object_id =
                OBJECT_ID(
                    N'dbo.ReconciliationRuns'
                )
        )
        BEGIN
            CREATE INDEX
                IX_ReconciliationRuns_Status
            ON dbo.ReconciliationRuns
            (
                Status,
                RunDate DESC
            );
        END;
        """
    )


    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.ReconciliationRunFiles',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.ReconciliationRunFiles
            (
                RunFileID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_ReconciliationRunFiles PRIMARY KEY,
                RunID BIGINT NOT NULL,
                FileCategory NVARCHAR(30) NOT NULL,
                FileType NVARCHAR(50) NULL,
                FileName NVARCHAR(500) NOT NULL,
                ContainerName NVARCHAR(100) NOT NULL,
                BlobName NVARCHAR(1000) NOT NULL,
                ContentType NVARCHAR(200) NULL,
                SizeBytes BIGINT NOT NULL
                    CONSTRAINT DF_ReconciliationRunFiles_SizeBytes DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_ReconciliationRunFiles_CreatedAt DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_ReconciliationRunFiles_Run
                    FOREIGN KEY (RunID) REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_ReconciliationRunFiles_Blob
                    UNIQUE (RunID, ContainerName, BlobName)
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_ReconciliationRunFiles_RunID'
              AND object_id = OBJECT_ID(N'dbo.ReconciliationRunFiles')
        )
        BEGIN
            CREATE INDEX IX_ReconciliationRunFiles_RunID
            ON dbo.ReconciliationRunFiles (RunID, FileCategory, CreatedAt);
        END;
        """
    )


    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.BatchEvents',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.BatchEvents
            (
                BatchEventID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_BatchEvents PRIMARY KEY,

                RunID BIGINT NOT NULL,
                EventKey NVARCHAR(200) NOT NULL,
                BN NVARCHAR(200) NOT NULL,
                ExpiryDate DATE NOT NULL,
                EventType NVARCHAR(50) NOT NULL,
                EventDate DATETIME2(3) NULL,
                Quantity DECIMAL(19,4) NOT NULL
                    CONSTRAINT DF_BatchEvents_Quantity DEFAULT 0,
                SourceSystem NVARCHAR(50) NOT NULL,
                SourceReference NVARCHAR(500) NULL,
                GenericItemNumber NVARCHAR(200) NULL,
                TradeItemNumber NVARCHAR(200) NULL,
                TradeName NVARCHAR(500) NULL,
                GTIN NVARCHAR(100) NULL,
                SupplierName NVARCHAR(500) NULL,
                CustomerName NVARCHAR(500) NULL,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_BatchEvents_CreatedAt
                    DEFAULT SYSUTCDATETIME(),

                CONSTRAINT FK_BatchEvents_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),

                CONSTRAINT UQ_BatchEvents_EventKey
                    UNIQUE (EventKey)
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_BatchEvents_BatchExpiry'
              AND object_id = OBJECT_ID(N'dbo.BatchEvents')
        )
        BEGIN
            CREATE INDEX IX_BatchEvents_BatchExpiry
            ON dbo.BatchEvents
            (
                BN,
                ExpiryDate,
                EventDate,
                BatchEventID
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_BatchEvents_RunID'
              AND object_id = OBJECT_ID(N'dbo.BatchEvents')
        )
        BEGIN
            CREATE INDEX IX_BatchEvents_RunID
            ON dbo.BatchEvents
            (
                RunID,
                EventType,
                CreatedAt
            );
        END;
        """
    )



    # ---------------------------------------------------------
    # SFDA upload verification lifecycle
    # ---------------------------------------------------------

    database.execute(
        """
        IF COL_LENGTH(
            N'dbo.ReconciliationRuns',
            N'VerificationStatus'
        ) IS NULL
        BEGIN
            ALTER TABLE dbo.ReconciliationRuns
            ADD VerificationStatus NVARCHAR(50) NOT NULL
                CONSTRAINT DF_ReconciliationRuns_VerificationStatus
                DEFAULT N'Not Started';
        END;
        """
    )

    database.execute(
        """
        IF COL_LENGTH(
            N'dbo.ReconciliationRuns',
            N'VerificationStartedAt'
        ) IS NULL
        BEGIN
            ALTER TABLE dbo.ReconciliationRuns
            ADD VerificationStartedAt DATETIME2(3) NULL;
        END;
        """
    )

    database.execute(
        """
        IF COL_LENGTH(
            N'dbo.ReconciliationRuns',
            N'VerifiedAt'
        ) IS NULL
        BEGIN
            ALTER TABLE dbo.ReconciliationRuns
            ADD VerifiedAt DATETIME2(3) NULL;
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.ReconciliationActions',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.ReconciliationActions
            (
                ActionID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_ReconciliationActions PRIMARY KEY,
                RunID BIGINT NOT NULL,
                ActionKey NVARCHAR(500) NOT NULL,
                ActionType NVARCHAR(20) NOT NULL,
                GeneratedFileName NVARCHAR(500) NULL,
                GeneratedRowNumber INT NULL,
                GTIN NVARCHAR(50) NOT NULL,
                BN NVARCHAR(150) NOT NULL,
                ExpiryDate DATE NOT NULL,
                Quantity DECIMAL(18,3) NOT NULL,
                Status NVARCHAR(50) NOT NULL
                    CONSTRAINT DF_ReconciliationActions_Status
                    DEFAULT N'PENDING',
                ResultCode NVARCHAR(50) NULL,
                ResultDescription NVARCHAR(2000) NULL,
                NotificationResultID BIGINT NULL,
                VerifiedAt DATETIME2(3) NULL,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_ReconciliationActions_CreatedAt
                    DEFAULT SYSUTCDATETIME(),
                UpdatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_ReconciliationActions_UpdatedAt
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_ReconciliationActions_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_ReconciliationActions_ActionKey
                    UNIQUE (ActionKey),
                CONSTRAINT CK_ReconciliationActions_ActionType
                    CHECK (ActionType IN (N'ACCEPT', N'DISPATCH')),
                CONSTRAINT CK_ReconciliationActions_Status
                    CHECK (Status IN
                    (
                        N'PENDING',
                        N'SUCCESS',
                        N'FAILED',
                        N'CONFLICT',
                        N'UNMATCHED'
                    ))
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_ReconciliationActions_RunStatus'
              AND object_id = OBJECT_ID(N'dbo.ReconciliationActions')
        )
        BEGIN
            CREATE INDEX IX_ReconciliationActions_RunStatus
            ON dbo.ReconciliationActions
            (
                RunID,
                ActionType,
                Status,
                BN,
                ExpiryDate
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.SFDAVerifications',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.SFDAVerifications
            (
                VerificationID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_SFDAVerifications PRIMARY KEY,
                RunID BIGINT NOT NULL,
                VerificationNumber NVARCHAR(60) NOT NULL,
                SubmittedBy NVARCHAR(200) NULL,
                Status NVARCHAR(50) NOT NULL
                    CONSTRAINT DF_SFDAVerifications_Status
                    DEFAULT N'Processing',
                NotificationFiles INT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_NotificationFiles
                    DEFAULT 0,
                NotificationRows BIGINT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_NotificationRows
                    DEFAULT 0,
                SuccessRows BIGINT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_SuccessRows
                    DEFAULT 0,
                FailedRows BIGINT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_FailedRows
                    DEFAULT 0,
                ConflictRows BIGINT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_ConflictRows
                    DEFAULT 0,
                UnmatchedRows BIGINT NOT NULL
                    CONSTRAINT DF_SFDAVerifications_UnmatchedRows
                    DEFAULT 0,
                ErrorMessage NVARCHAR(MAX) NULL,
                StartedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_SFDAVerifications_StartedAt
                    DEFAULT SYSUTCDATETIME(),
                CompletedAt DATETIME2(3) NULL,
                CONSTRAINT FK_SFDAVerifications_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_SFDAVerifications_Number
                    UNIQUE (VerificationNumber),
                CONSTRAINT CK_SFDAVerifications_Status
                    CHECK (Status IN
                    (
                        N'Processing',
                        N'Completed',
                        N'Failed'
                    ))
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.SFDANotificationFiles',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.SFDANotificationFiles
            (
                NotificationFileID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_SFDANotificationFiles PRIMARY KEY,
                VerificationID BIGINT NOT NULL,
                RunID BIGINT NOT NULL,
                FileName NVARCHAR(500) NOT NULL,
                ContainerName NVARCHAR(100) NULL,
                BlobName NVARCHAR(1000) NULL,
                ContentType NVARCHAR(200) NULL,
                SizeBytes BIGINT NOT NULL
                    CONSTRAINT DF_SFDANotificationFiles_SizeBytes
                    DEFAULT 0,
                FileHash NVARCHAR(128) NULL,
                RowCount BIGINT NOT NULL
                    CONSTRAINT DF_SFDANotificationFiles_RowCount
                    DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_SFDANotificationFiles_CreatedAt
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_SFDANotificationFiles_Verification
                    FOREIGN KEY (VerificationID)
                    REFERENCES dbo.SFDAVerifications(VerificationID),
                CONSTRAINT FK_SFDANotificationFiles_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_SFDANotificationFiles_Hash
                    UNIQUE (RunID, FileHash)
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.SFDANotificationResults',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.SFDANotificationResults
            (
                NotificationResultID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_SFDANotificationResults PRIMARY KEY,
                NotificationFileID BIGINT NOT NULL,
                VerificationID BIGINT NOT NULL,
                RunID BIGINT NOT NULL,
                RowNumber INT NOT NULL,
                GTIN NVARCHAR(50) NOT NULL,
                Quantity DECIMAL(18,3) NOT NULL,
                BN NVARCHAR(150) NOT NULL,
                ExpiryDate DATE NOT NULL,
                ResultCode NVARCHAR(50) NULL,
                Description NVARCHAR(2000) NULL,
                InterpretedStatus NVARCHAR(50) NOT NULL,
                MatchedActionID BIGINT NULL,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_SFDANotificationResults_CreatedAt
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_SFDANotificationResults_File
                    FOREIGN KEY (NotificationFileID)
                    REFERENCES dbo.SFDANotificationFiles(NotificationFileID),
                CONSTRAINT FK_SFDANotificationResults_Verification
                    FOREIGN KEY (VerificationID)
                    REFERENCES dbo.SFDAVerifications(VerificationID),
                CONSTRAINT FK_SFDANotificationResults_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT FK_SFDANotificationResults_Action
                    FOREIGN KEY (MatchedActionID)
                    REFERENCES dbo.ReconciliationActions(ActionID),
                CONSTRAINT UQ_SFDANotificationResults_Row
                    UNIQUE (NotificationFileID, RowNumber),
                CONSTRAINT CK_SFDANotificationResults_Status
                    CHECK (InterpretedStatus IN
                    (
                        N'SUCCESS',
                        N'FAILED',
                        N'CONFLICT',
                        N'UNMATCHED'
                    ))
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.SFDAReportSnapshots',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.SFDAReportSnapshots
            (
                SnapshotID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_SFDAReportSnapshots PRIMARY KEY,
                VerificationID BIGINT NOT NULL,
                RunID BIGINT NOT NULL,
                FileName NVARCHAR(500) NOT NULL,
                ContainerName NVARCHAR(100) NULL,
                BlobName NVARCHAR(1000) NULL,
                ContentType NVARCHAR(200) NULL,
                SizeBytes BIGINT NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshots_SizeBytes
                    DEFAULT 0,
                FileHash NVARCHAR(128) NULL,
                RowCount BIGINT NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshots_RowCount
                    DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshots_CreatedAt
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_SFDAReportSnapshots_Verification
                    FOREIGN KEY (VerificationID)
                    REFERENCES dbo.SFDAVerifications(VerificationID),
                CONSTRAINT FK_SFDAReportSnapshots_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_SFDAReportSnapshots_Hash
                    UNIQUE (RunID, FileHash)
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.SFDAReportSnapshotRows',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.SFDAReportSnapshotRows
            (
                SnapshotRowID BIGINT IDENTITY(1,1) NOT NULL
                    CONSTRAINT PK_SFDAReportSnapshotRows PRIMARY KEY,
                SnapshotID BIGINT NOT NULL,
                RunID BIGINT NOT NULL,
                GTIN NVARCHAR(50) NOT NULL,
                DrugName NVARCHAR(500) NULL,
                BN NVARCHAR(150) NOT NULL,
                ExpiryDate DATE NOT NULL,
                Quantity DECIMAL(18,3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshotRows_Quantity
                    DEFAULT 0,
                Active DECIMAL(18,3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshotRows_Active
                    DEFAULT 0,
                QuantitySentPending DECIMAL(18,3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshotRows_SentPending
                    DEFAULT 0,
                QuantityReceivePending DECIMAL(18,3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshotRows_ReceivePending
                    DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    CONSTRAINT DF_SFDAReportSnapshotRows_CreatedAt
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_SFDAReportSnapshotRows_Snapshot
                    FOREIGN KEY (SnapshotID)
                    REFERENCES dbo.SFDAReportSnapshots(SnapshotID),
                CONSTRAINT FK_SFDAReportSnapshotRows_Run
                    FOREIGN KEY (RunID)
                    REFERENCES dbo.ReconciliationRuns(RunID),
                CONSTRAINT UQ_SFDAReportSnapshotRows_Key
                    UNIQUE (SnapshotID, GTIN, BN, ExpiryDate)
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_SFDANotificationResults_RunKey'
              AND object_id = OBJECT_ID(N'dbo.SFDANotificationResults')
        )
        BEGIN
            CREATE INDEX IX_SFDANotificationResults_RunKey
            ON dbo.SFDANotificationResults
            (
                RunID,
                GTIN,
                BN,
                ExpiryDate,
                InterpretedStatus
            );
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'IX_SFDAReportSnapshotRows_RunKey'
              AND object_id = OBJECT_ID(N'dbo.SFDAReportSnapshotRows')
        )
        BEGIN
            CREATE INDEX IX_SFDAReportSnapshotRows_RunKey
            ON dbo.SFDAReportSnapshotRows
            (
                RunID,
                GTIN,
                BN,
                ExpiryDate
            );
        END;
        """
    )

def _generate_run_number() -> str:
    now = datetime.now(
        timezone.utc
    )

    return now.strftime(
        "RUN-%Y%m%d-%H%M%S-%f"
    )[:-3]


def create_reconciliation_run(
    submitted_by: str = "Web User",
    application_version: Optional[str] = None,
    asn_files: int = 1,
    inventory_files: int = 1,
    dispatch_files: int = 1,
    sfda_files: int = 1,
) -> Dict[str, Any]:

    initialize_database()

    database = Database()

    run_number = _generate_run_number()

    run_id = database.execute_scalar(
        """
        INSERT INTO dbo.ReconciliationRuns
        (
            RunNumber,
            SubmittedBy,
            ASNFiles,
            DispatchFiles,
            InventoryFiles,
            SFDAFiles,
            ApplicationVersion,
            Status,
            StartedAt
        )
        OUTPUT INSERTED.RunID
        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            N'Processing',
            SYSUTCDATETIME()
        );
        """,
        (
            run_number,
            submitted_by,
            asn_files,
            dispatch_files,
            inventory_files,
            sfda_files,
            application_version,
        ),
    )

    return {
        "run_id": int(run_id),
        "run_number": run_number,
        "status": "Processing",
    }


def complete_reconciliation_run(
    run_id: int,
    total_input_rows: int,
    master_records: int,
    accept_records: int,
    dispatch_records: int,
    exception_records: int,
    generated_files: int,
) -> None:

    database = Database()

    database.execute(
        """
        UPDATE dbo.ReconciliationRuns
        SET
            TotalInputRows = %s,
            MasterRecords = %s,
            AcceptRecords = %s,
            DispatchRecords = %s,
            ExceptionRecords = %s,
            GeneratedFiles = %s,
            Status = N'Completed',
            ErrorMessage = NULL,
            CompletedAt =
                SYSUTCDATETIME()
        WHERE RunID = %s;
        """,
        (
            total_input_rows,
            master_records,
            accept_records,
            dispatch_records,
            exception_records,
            generated_files,
            run_id,
        ),
    )


def fail_reconciliation_run(
    run_id: int,
    error_message: str,
) -> None:

    database = Database()

    database.execute(
        """
        UPDATE dbo.ReconciliationRuns
        SET
            Status = N'Failed',
            ErrorMessage = %s,
            CompletedAt =
                SYSUTCDATETIME()
        WHERE RunID = %s;
        """,
        (
            str(error_message)[:4000],
            run_id,
        ),
    )


def get_reconciliation_history(
    limit: int = 100,
) -> List[Dict[str, Any]]:

    database = Database()

    safe_limit = max(
        1,
        min(
            int(limit),
            500
        )
    )

    return database.fetch_all(
        f"""
        SELECT TOP ({safe_limit})
            RunID,
            RunNumber,
            RunDate,
            SubmittedBy,
            ASNFiles,
            DispatchFiles,
            InventoryFiles,
            SFDAFiles,
            TotalInputRows,
            MasterRecords,
            AcceptRecords,
            DispatchRecords,
            ExceptionRecords,
            GeneratedFiles,
            ApplicationVersion,
            Status,
            ErrorMessage,
            StartedAt,
            CompletedAt,
            VerificationStatus,
            VerificationStartedAt,
            VerifiedAt
        FROM dbo.ReconciliationRuns
        ORDER BY RunID DESC;
        """
    )


def test_database_connection() -> Dict[str, Any]:
    database = Database()

    result = database.fetch_one(
        """
        SELECT
            DB_NAME() AS DatabaseName,
            @@SERVERNAME AS ServerName,
            SYSUTCDATETIME()
                AS ServerUtcTime;
        """
    )

    if result is None:
        raise RuntimeError(
            "Database connection succeeded "
            "but returned no test result."
        )

    return {
        "status": "Connected",
        "database": result.get(
            "DatabaseName"
        ),
        "server": result.get(
            "ServerName"
        ),
        "server_utc_time": result.get(
            "ServerUtcTime"
        ),
    }

def record_reconciliation_file(
    run_id: int,
    file_category: str,
    file_type: str,
    file_name: str,
    container_name: str,
    blob_name: str,
    content_type: str,
    size_bytes: int,
) -> None:
    database = Database()
    database.execute(
        """
        MERGE dbo.ReconciliationRunFiles AS target
        USING
        (
            SELECT
                %s AS RunID,
                %s AS ContainerName,
                %s AS BlobName
        ) AS source
        ON target.RunID = source.RunID
           AND target.ContainerName = source.ContainerName
           AND target.BlobName = source.BlobName
        WHEN MATCHED THEN
            UPDATE SET
                FileCategory = %s,
                FileType = %s,
                FileName = %s,
                ContentType = %s,
                SizeBytes = %s
        WHEN NOT MATCHED THEN
            INSERT
            (
                RunID, FileCategory, FileType, FileName,
                ContainerName, BlobName, ContentType, SizeBytes
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """,
        (
            run_id, container_name, blob_name,
            file_category, file_type, file_name, content_type, size_bytes,
            run_id, file_category, file_type, file_name,
            container_name, blob_name, content_type, size_bytes,
        ),
    )


def get_reconciliation_run_by_number(
    run_number: str,
) -> Optional[Dict[str, Any]]:
    database = Database()
    return database.fetch_one(
        """
        SELECT
            RunID, RunNumber, RunDate, SubmittedBy,
            ASNFiles, DispatchFiles, InventoryFiles, SFDAFiles,
            TotalInputRows, MasterRecords, AcceptRecords,
            DispatchRecords, ExceptionRecords, GeneratedFiles,
            ApplicationVersion, Status, ErrorMessage,
            StartedAt, CompletedAt, VerificationStatus,
            VerificationStartedAt, VerifiedAt
        FROM dbo.ReconciliationRuns
        WHERE RunNumber = %s;
        """,
        (run_number,),
    )


def get_reconciliation_run_files(
    run_id: int,
) -> List[Dict[str, Any]]:
    database = Database()
    return database.fetch_all(
        """
        SELECT
            RunFileID, RunID, FileCategory, FileType, FileName,
            ContainerName, BlobName, ContentType, SizeBytes, CreatedAt
        FROM dbo.ReconciliationRunFiles
        WHERE RunID = %s
        ORDER BY
            CASE FileCategory
                WHEN N'input' THEN 1
                WHEN N'output' THEN 2
                WHEN N'metadata' THEN 3
                ELSE 4
            END,
            FileName;
        """,
        (run_id,),
    )


def record_batch_events(
    run_id: int,
    events: List[Dict[str, Any]],
) -> int:

    if not events:
        return 0

    database = Database()
    rows = []

    for event in events:
        event_key = str(event.get("event_key") or "").strip()
        batch_number = str(event.get("bn") or "").strip()
        expiry_date = event.get("expiry_date")
        event_type = str(event.get("event_type") or "").strip().upper()
        source_system = str(event.get("source_system") or "").strip().upper()

        if not event_key:
            raise ValueError("Batch event is missing event_key.")
        if not batch_number:
            raise ValueError("Batch event is missing BN.")
        if expiry_date is None:
            raise ValueError("Batch event is missing expiry_date.")
        if not event_type:
            raise ValueError("Batch event is missing event_type.")
        if not source_system:
            raise ValueError("Batch event is missing source_system.")

        row = (
            int(run_id),
            event_key,
            batch_number,
            expiry_date,
            event_type,
            event.get("event_date"),
            float(event.get("quantity") or 0),
            source_system,
            event.get("source_reference"),
            event.get("generic_item_number"),
            event.get("trade_item_number"),
            event.get("trade_name"),
            event.get("gtin"),
            event.get("supplier_name"),
            event.get("customer_name"),
        )
        rows.append((event_key,) + row)

    database.execute_many(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM dbo.BatchEvents
            WHERE EventKey = %s
        )
        BEGIN
            INSERT INTO dbo.BatchEvents
            (
                RunID, EventKey, BN, ExpiryDate, EventType,
                EventDate, Quantity, SourceSystem, SourceReference,
                GenericItemNumber, TradeItemNumber, TradeName, GTIN,
                SupplierName, CustomerName
            )
            VALUES
            (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            );
        END;
        """,
        rows,
    )

    return len(events)


def get_batch_events(
    batch_number: str,
    expiry_date: Optional[Any] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:

    database = Database()
    safe_limit = max(1, min(int(limit), 5000))

    if expiry_date is None:
        return database.fetch_all(
            f"""
            SELECT TOP ({safe_limit})
                BatchEventID, RunID, EventKey, BN, ExpiryDate,
                EventType, EventDate, Quantity, SourceSystem,
                SourceReference, GenericItemNumber, TradeItemNumber,
                TradeName, GTIN, SupplierName, CustomerName, CreatedAt
            FROM dbo.BatchEvents
            WHERE BN = %s
            ORDER BY COALESCE(EventDate, CreatedAt), BatchEventID;
            """,
            (batch_number,),
        )

    return database.fetch_all(
        f"""
        SELECT TOP ({safe_limit})
            BatchEventID, RunID, EventKey, BN, ExpiryDate,
            EventType, EventDate, Quantity, SourceSystem,
            SourceReference, GenericItemNumber, TradeItemNumber,
            TradeName, GTIN, SupplierName, CustomerName, CreatedAt
        FROM dbo.BatchEvents
        WHERE BN = %s AND ExpiryDate = %s
        ORDER BY COALESCE(EventDate, CreatedAt), BatchEventID;
        """,
        (batch_number, expiry_date),
    )


def get_batch_event_totals(
    batch_number: str,
    expiry_date: Any,
) -> Dict[str, Any]:

    database = Database()
    rows = database.fetch_all(
        """
        SELECT
            EventType,
            SUM(Quantity) AS TotalQuantity,
            COUNT_BIG(*) AS EventCount,
            MIN(EventDate) AS FirstEventDate,
            MAX(EventDate) AS LastEventDate
        FROM dbo.BatchEvents
        WHERE BN = %s AND ExpiryDate = %s
        GROUP BY EventType
        ORDER BY EventType;
        """,
        (batch_number, expiry_date),
    )

    return {
        "batch_number": batch_number,
        "expiry_date": expiry_date,
        "events": rows,
    }
