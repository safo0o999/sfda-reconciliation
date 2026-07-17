import os
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
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
        IF COL_LENGTH(
            N'dbo.BatchEvents',
            N'UpdatedAt'
        ) IS NULL
        BEGIN
            ALTER TABLE dbo.BatchEvents
            ADD UpdatedAt DATETIME2(3) NULL;
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



    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.VerificationRuns',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.VerificationRuns
            (
                VerificationID BIGINT
                    IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT
                        PK_VerificationRuns
                    PRIMARY KEY,
                RunID BIGINT NOT NULL,
                Status NVARCHAR(50) NOT NULL,
                LatestSFDAFileName
                    NVARCHAR(500) NULL,
                NotificationFiles INT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Files
                    DEFAULT 0,
                ExpectedRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Expected
                    DEFAULT 0,
                NotificationRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Notification
                    DEFAULT 0,
                VerifiedRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Verified
                    DEFAULT 0,
                RejectedRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Rejected
                    DEFAULT 0,
                UnclassifiedRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Unclassified
                    DEFAULT 0,
                MismatchRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Mismatch
                    DEFAULT 0,
                MissingExpectedRows BIGINT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Missing
                    DEFAULT 0,
                ErrorMessage NVARCHAR(MAX) NULL,
                CreatedAt DATETIME2(3)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationRuns_Created
                    DEFAULT SYSUTCDATETIME(),
                CompletedAt DATETIME2(3) NULL,
                CONSTRAINT
                    FK_VerificationRuns_Run
                FOREIGN KEY (RunID)
                REFERENCES
                    dbo.ReconciliationRuns(RunID)
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.VerificationResults',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.VerificationResults
            (
                VerificationResultID BIGINT
                    IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT
                        PK_VerificationResults
                    PRIMARY KEY,
                VerificationID BIGINT
                    NOT NULL,
                NotificationFile
                    NVARCHAR(500) NULL,
                NotificationRow INT NULL,
                NotificationType
                    NVARCHAR(30) NULL,
                ClassificationStatus
                    NVARCHAR(100) NULL,
                GTIN NVARCHAR(100) NULL,
                BN NVARCHAR(200) NULL,
                ExpiryDate DATE NULL,
                Quantity DECIMAL(19,4)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Qty
                    DEFAULT 0,
                ResultCode NVARCHAR(50) NULL,
                Description NVARCHAR(2000) NULL,
                PortalSuccess BIT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Success
                    DEFAULT 0,
                OriginalActive DECIMAL(19,4)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Original
                    DEFAULT 0,
                LatestActive DECIMAL(19,4)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Latest
                    DEFAULT 0,
                ExpectedActive DECIMAL(19,4)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Expected
                    DEFAULT 0,
                ActiveMatches BIT
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Matches
                    DEFAULT 0,
                VerificationStatus
                    NVARCHAR(100) NULL,
                CreatedAt DATETIME2(3)
                    NOT NULL
                    CONSTRAINT
                        DF_VerificationResults_Created
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT
                    FK_VerificationResults_Run
                FOREIGN KEY (VerificationID)
                REFERENCES
                    dbo.VerificationRuns(
                        VerificationID
                    )
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


def update_reconciliation_run_status(
    run_id: int,
    status: str,
) -> None:
    database = Database()
    database.execute(
        """
        UPDATE dbo.ReconciliationRuns
        SET
            Status = %s,
            ErrorMessage = NULL,
            CompletedAt =
                CASE
                    WHEN %s IN
                    (
                        N'Verified',
                        N'Investigation Required'
                    )
                    THEN SYSUTCDATETIME()
                    ELSE CompletedAt
                END
        WHERE RunID = %s;
        """,
        (
            status,
            status,
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
            CompletedAt
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
            StartedAt, CompletedAt
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
    batch_size: int = 1000,
) -> int:

    if not events:
        logging.info(
            "[BATCH EVENTS] No events to save "
            "| run_id=%s",
            run_id,
        )
        return 0

    safe_batch_size = max(
        250,
        min(
            int(batch_size),
            5000,
        ),
    )

    rows = []

    for event in events:
        event_key = str(
            event.get("event_key") or ""
        ).strip()
        batch_number = str(
            event.get("bn") or ""
        ).strip()
        expiry_date = event.get("expiry_date")
        event_type = str(
            event.get("event_type") or ""
        ).strip().upper()
        source_system = str(
            event.get("source_system") or ""
        ).strip().upper()

        if not event_key:
            raise ValueError(
                "Batch event is missing event_key."
            )

        if not batch_number:
            raise ValueError(
                "Batch event is missing BN."
            )

        if expiry_date is None:
            raise ValueError(
                "Batch event is missing expiry_date."
            )

        if not event_type:
            raise ValueError(
                "Batch event is missing event_type."
            )

        if not source_system:
            raise ValueError(
                "Batch event is missing source_system."
            )

        rows.append((
            event_key,
            int(run_id),
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
        ))

    total_rows = len(rows)
    total_batches = (
        total_rows
        + safe_batch_size
        - 1
    ) // safe_batch_size
    operation_started_at = time.perf_counter()

    database = Database()
    connection = database.connect()

    create_stage_sql = """
        CREATE TABLE #BatchEventsStage
        (
            EventKey NVARCHAR(200) NOT NULL,
            RunID BIGINT NOT NULL,
            BN NVARCHAR(200) NOT NULL,
            ExpiryDate DATE NOT NULL,
            EventType NVARCHAR(50) NOT NULL,
            EventDate DATETIME2(3) NULL,
            Quantity DECIMAL(19,4) NOT NULL,
            SourceSystem NVARCHAR(50) NOT NULL,
            SourceReference NVARCHAR(500) NULL,
            GenericItemNumber NVARCHAR(200) NULL,
            TradeItemNumber NVARCHAR(200) NULL,
            TradeName NVARCHAR(500) NULL,
            GTIN NVARCHAR(100) NULL,
            SupplierName NVARCHAR(500) NULL,
            CustomerName NVARCHAR(500) NULL
        );

        CREATE UNIQUE CLUSTERED INDEX
            IX_BatchEventsStage_EventKey
        ON #BatchEventsStage (EventKey);
    """

    insert_stage_sql = """
        INSERT INTO #BatchEventsStage
        (
            EventKey,
            RunID,
            BN,
            ExpiryDate,
            EventType,
            EventDate,
            Quantity,
            SourceSystem,
            SourceReference,
            GenericItemNumber,
            TradeItemNumber,
            TradeName,
            GTIN,
            SupplierName,
            CustomerName
        )
        VALUES
        (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        );
    """

    merge_sql = """
        MERGE dbo.BatchEvents WITH (HOLDLOCK) AS target
        USING #BatchEventsStage AS source
            ON target.EventKey = source.EventKey

        WHEN MATCHED THEN
            UPDATE SET
                RunID = source.RunID,
                BN = source.BN,
                ExpiryDate = source.ExpiryDate,
                EventType = source.EventType,
                EventDate = source.EventDate,
                Quantity = source.Quantity,
                SourceSystem = source.SourceSystem,
                SourceReference = source.SourceReference,
                GenericItemNumber = source.GenericItemNumber,
                TradeItemNumber = source.TradeItemNumber,
                TradeName = source.TradeName,
                GTIN = source.GTIN,
                SupplierName = source.SupplierName,
                CustomerName = source.CustomerName,
                UpdatedAt = SYSUTCDATETIME()

        WHEN NOT MATCHED THEN
            INSERT
            (
                RunID,
                EventKey,
                BN,
                ExpiryDate,
                EventType,
                EventDate,
                Quantity,
                SourceSystem,
                SourceReference,
                GenericItemNumber,
                TradeItemNumber,
                TradeName,
                GTIN,
                SupplierName,
                CustomerName,
                UpdatedAt
            )
            VALUES
            (
                source.RunID,
                source.EventKey,
                source.BN,
                source.ExpiryDate,
                source.EventType,
                source.EventDate,
                source.Quantity,
                source.SourceSystem,
                source.SourceReference,
                source.GenericItemNumber,
                source.TradeItemNumber,
                source.TradeName,
                source.GTIN,
                source.SupplierName,
                source.CustomerName,
                SYSUTCDATETIME()
            );
    """

    logging.info(
        "[BATCH EVENTS] Starting optimized bulk upsert "
        "| run_id=%s | total_rows=%s "
        "| batch_size=%s | total_batches=%s",
        run_id,
        total_rows,
        safe_batch_size,
        total_batches,
    )

    try:
        cursor = connection.cursor()

        stage_started_at = time.perf_counter()
        cursor.execute(create_stage_sql)

        logging.info(
            "[BATCH EVENTS] Temporary staging table created "
            "| run_id=%s | seconds=%.3f",
            run_id,
            time.perf_counter() - stage_started_at,
        )

        inserted_rows = 0

        for batch_number_index, start_index in enumerate(
            range(
                0,
                total_rows,
                safe_batch_size,
            ),
            start=1,
        ):
            batch_started_at = time.perf_counter()
            batch_rows = rows[
                start_index:
                start_index + safe_batch_size
            ]

            cursor.executemany(
                insert_stage_sql,
                batch_rows,
            )

            inserted_rows += len(batch_rows)

            logging.info(
                "[BATCH EVENTS] Staging batch inserted "
                "| run_id=%s | batch=%s/%s "
                "| inserted_rows=%s/%s | seconds=%.3f",
                run_id,
                batch_number_index,
                total_batches,
                inserted_rows,
                total_rows,
                time.perf_counter() - batch_started_at,
            )

        merge_started_at = time.perf_counter()

        logging.info(
            "[BATCH EVENTS] Starting single SQL MERGE "
            "| run_id=%s | staged_rows=%s",
            run_id,
            inserted_rows,
        )

        cursor.execute(merge_sql)
        connection.commit()

        logging.info(
            "[BATCH EVENTS] Single SQL MERGE completed "
            "| run_id=%s | rows=%s | seconds=%.3f",
            run_id,
            inserted_rows,
            time.perf_counter() - merge_started_at,
        )

    except Exception:
        connection.rollback()

        logging.exception(
            "[BATCH EVENTS] Optimized bulk upsert failed "
            "| run_id=%s | total_rows=%s",
            run_id,
            total_rows,
        )

        raise

    finally:
        connection.close()

    logging.info(
        "[BATCH EVENTS] All events saved "
        "| run_id=%s | saved_rows=%s "
        "| total_seconds=%.3f",
        run_id,
        total_rows,
        time.perf_counter() - operation_started_at,
    )

    return total_rows

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
                TradeName, GTIN, SupplierName, CustomerName, CreatedAt, UpdatedAt
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
            TradeName, GTIN, SupplierName, CustomerName, CreatedAt, UpdatedAt
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

def create_verification_run(
    run_id: int,
    latest_sfda_file_name: str,
    notification_files: int,
) -> int:
    initialize_database()
    database = Database()

    verification_id = database.execute_scalar(
        """
        INSERT INTO dbo.VerificationRuns
        (
            RunID,
            Status,
            LatestSFDAFileName,
            NotificationFiles
        )
        OUTPUT INSERTED.VerificationID
        VALUES
        (
            %s,
            N'Processing',
            %s,
            %s
        );
        """,
        (
            run_id,
            latest_sfda_file_name,
            notification_files,
        ),
    )

    return int(verification_id)


def complete_verification_run(
    verification_id: int,
    status: str,
    summary: Dict[str, Any],
) -> None:
    database = Database()
    database.execute(
        """
        UPDATE dbo.VerificationRuns
        SET
            Status = %s,
            ExpectedRows = %s,
            NotificationRows = %s,
            VerifiedRows = %s,
            RejectedRows = %s,
            UnclassifiedRows = %s,
            MismatchRows = %s,
            MissingExpectedRows = %s,
            ErrorMessage = NULL,
            CompletedAt = SYSUTCDATETIME()
        WHERE VerificationID = %s;
        """,
        (
            status,
            int(summary.get(
                "expected_rows",
                0,
            )),
            int(summary.get(
                "notification_rows",
                0,
            )),
            int(summary.get(
                "verified_rows",
                0,
            )),
            int(summary.get(
                "rejected_rows",
                0,
            )),
            int(summary.get(
                "unclassified_rows",
                0,
            )),
            int(summary.get(
                "mismatch_rows",
                0,
            )),
            int(summary.get(
                "missing_expected_rows",
                0,
            )),
            verification_id,
        ),
    )


def fail_verification_run(
    verification_id: int,
    error_message: str,
) -> None:
    database = Database()
    database.execute(
        """
        UPDATE dbo.VerificationRuns
        SET
            Status = N'Failed',
            ErrorMessage = %s,
            CompletedAt = SYSUTCDATETIME()
        WHERE VerificationID = %s;
        """,
        (
            str(error_message)[:4000],
            verification_id,
        ),
    )


def record_verification_results(
    verification_id: int,
    rows: List[Dict[str, Any]],
) -> int:
    if not rows:
        return 0

    database = Database()
    parameters = []

    for row in rows:
        parameters.append((
            verification_id,
            row.get("notification_file"),
            row.get("notification_row"),
            row.get("notification_type"),
            row.get("classification_status"),
            row.get("gtin"),
            row.get("bn"),
            row.get("expiry_date"),
            float(row.get("quantity") or 0),
            row.get("result_code"),
            row.get("description"),
            1 if row.get("portal_success") else 0,
            float(row.get("original_active") or 0),
            float(row.get("latest_active") or 0),
            float(row.get("expected_active") or 0),
            1 if row.get("active_matches") else 0,
            row.get("verification_status"),
        ))

    database.execute_many(
        """
        INSERT INTO dbo.VerificationResults
        (
            VerificationID,
            NotificationFile,
            NotificationRow,
            NotificationType,
            ClassificationStatus,
            GTIN,
            BN,
            ExpiryDate,
            Quantity,
            ResultCode,
            Description,
            PortalSuccess,
            OriginalActive,
            LatestActive,
            ExpectedActive,
            ActiveMatches,
            VerificationStatus
        )
        VALUES
        (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        );
        """,
        parameters,
    )

    return len(rows)

# =====================================================================
# Full Reconciliation / Historical Batch Master
# =====================================================================

def initialize_full_reconciliation_database() -> None:
    database = Database()

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.FullReconciliationRuns',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.FullReconciliationRuns
            (
                FullRunID BIGINT IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT PK_FullReconciliationRuns
                    PRIMARY KEY,
                RunNumber NVARCHAR(60) NOT NULL,
                SubmittedBy NVARCHAR(200) NULL,
                ApplicationVersion NVARCHAR(30) NULL,
                Status NVARCHAR(50) NOT NULL,
                ASNFiles INT NOT NULL DEFAULT 0,
                DispatchFiles INT NOT NULL DEFAULT 0,
                SFDARows BIGINT NOT NULL DEFAULT 0,
                ReceiptEvents BIGINT NOT NULL DEFAULT 0,
                DispatchEvents BIGINT NOT NULL DEFAULT 0,
                MasterRecords BIGINT NOT NULL DEFAULT 0,
                BalancedRecords BIGINT NOT NULL DEFAULT 0,
                ReviewRecords BIGINT NOT NULL DEFAULT 0,
                MissingSFDARecords BIGINT NOT NULL DEFAULT 0,
                ErrorMessage NVARCHAR(MAX) NULL,
                StartedAt DATETIME2(3) NOT NULL
                    DEFAULT SYSUTCDATETIME(),
                CompletedAt DATETIME2(3) NULL,
                CONSTRAINT UQ_FullReconciliationRuns_RunNumber
                    UNIQUE (RunNumber)
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.FullReceiptEvents',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.FullReceiptEvents
            (
                ReceiptEventID BIGINT IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT PK_FullReceiptEvents
                    PRIMARY KEY,
                FullRunID BIGINT NOT NULL,
                EventKey NVARCHAR(64) NOT NULL,
                BN NVARCHAR(200) NOT NULL,
                ExpiryMonthKey CHAR(7) NOT NULL,
                WMSExpiryDate DATE NULL,
                EventDate DATE NULL,
                SourceFile NVARCHAR(500) NULL,
                SupplierName NVARCHAR(500) NULL,
                SupplierCode NVARCHAR(200) NULL,
                PONumber NVARCHAR(200) NULL,
                InvoiceNumber NVARCHAR(200) NULL,
                InboundShipment NVARCHAR(200) NULL,
                TradeName NVARCHAR(500) NULL,
                GenericItemNumber NVARCHAR(200) NULL,
                TradeItemNumber NVARCHAR(200) NULL,
                PackageSize DECIMAL(19,4) NOT NULL DEFAULT 1,
                QuantityUnits DECIMAL(19,4) NOT NULL DEFAULT 0,
                QuantityPackages DECIMAL(19,4) NOT NULL DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_FullReceiptEvents_Run
                    FOREIGN KEY (FullRunID)
                    REFERENCES dbo.FullReconciliationRuns(
                        FullRunID
                    )
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.FullDispatchEvents',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.FullDispatchEvents
            (
                DispatchEventID BIGINT IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT PK_FullDispatchEvents
                    PRIMARY KEY,
                FullRunID BIGINT NOT NULL,
                EventKey NVARCHAR(64) NOT NULL,
                BN NVARCHAR(200) NOT NULL,
                ExpiryMonthKey CHAR(7) NOT NULL,
                WMSExpiryDate DATE NULL,
                EventDate DATE NULL,
                SourceFile NVARCHAR(500) NULL,
                CustomerName NVARCHAR(500) NULL,
                SalesOrderNumber NVARCHAR(200) NULL,
                OrderLine NVARCHAR(100) NULL,
                TradeName NVARCHAR(500) NULL,
                GenericItemNumber NVARCHAR(200) NULL,
                TradeItemNumber NVARCHAR(200) NULL,
                PackageSize DECIMAL(19,4) NOT NULL DEFAULT 1,
                QuantityUnits DECIMAL(19,4) NOT NULL DEFAULT 0,
                QuantityPackages DECIMAL(19,4) NOT NULL DEFAULT 0,
                CreatedAt DATETIME2(3) NOT NULL
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_FullDispatchEvents_Run
                    FOREIGN KEY (FullRunID)
                    REFERENCES dbo.FullReconciliationRuns(
                        FullRunID
                    )
            );
        END;
        """
    )

    database.execute(
        """
        IF OBJECT_ID(
            N'dbo.BatchMaster',
            N'U'
        ) IS NULL
        BEGIN
            CREATE TABLE dbo.BatchMaster
            (
                BatchMasterID BIGINT IDENTITY(1,1)
                    NOT NULL
                    CONSTRAINT PK_BatchMaster
                    PRIMARY KEY,
                FullRunID BIGINT NOT NULL,
                BN NVARCHAR(200) NOT NULL,
                ExpiryMonthKey CHAR(7) NOT NULL,
                GTIN NVARCHAR(100) NULL,
                DrugName NVARCHAR(500) NULL,
                GenericItemNumber NVARCHAR(200) NULL,
                TradeItemNumber NVARCHAR(200) NULL,
                PackageSize DECIMAL(19,4) NOT NULL DEFAULT 1,
                WMSReceiptExpiryDate DATE NULL,
                WMSDispatchExpiryDate DATE NULL,
                SFDAExpiryDate DATE NULL,
                FirstReceiptDate DATE NULL,
                LastReceiptDate DATE NULL,
                FirstDispatchDate DATE NULL,
                LastDispatchDate DATE NULL,
                TotalReceivedUnits DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                TotalReceivedPackages DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                TotalDispatchedUnits DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                TotalDispatchedPackages DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                NetPhysicalPackages DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                SFDAQuantity DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                SFDAActive DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                SFDAReceivePending DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                SFDASendPending DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                PhysicalActiveVariance DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                HistoricalReceiptUncovered DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                HistoricalDispatchUncovered DECIMAL(19,4)
                    NOT NULL DEFAULT 0,
                MasterStatus NVARCHAR(50) NOT NULL,
                UpdatedAt DATETIME2(3) NOT NULL
                    DEFAULT SYSUTCDATETIME(),
                CONSTRAINT FK_BatchMaster_Run
                    FOREIGN KEY (FullRunID)
                    REFERENCES dbo.FullReconciliationRuns(
                        FullRunID
                    ),
                CONSTRAINT UQ_BatchMaster_BatchMonth
                    UNIQUE (BN, ExpiryMonthKey)
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
            WHERE name =
                N'IX_BatchMaster_Status'
              AND object_id =
                OBJECT_ID(N'dbo.BatchMaster')
        )
        BEGIN
            CREATE INDEX IX_BatchMaster_Status
            ON dbo.BatchMaster
            (
                MasterStatus,
                ExpiryMonthKey,
                BN
            );
        END;
        """
    )


def _generate_full_run_number() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime(
        "FULL-%Y%m%d-%H%M%S-%f"
    )[:-3]


def create_full_reconciliation_run(
    submitted_by: str = "Web User",
    application_version: Optional[str] = None,
    asn_files: int = 0,
    dispatch_files: int = 0,
    sfda_rows: int = 0,
) -> Dict[str, Any]:
    initialize_full_reconciliation_database()

    database = Database()
    run_number = _generate_full_run_number()

    run_id = database.execute_scalar(
        """
        INSERT INTO dbo.FullReconciliationRuns
        (
            RunNumber,
            SubmittedBy,
            ApplicationVersion,
            Status,
            ASNFiles,
            DispatchFiles,
            SFDARows,
            StartedAt
        )
        OUTPUT INSERTED.FullRunID
        VALUES
        (
            %s, %s, %s, N'Processing',
            %s, %s, %s, SYSUTCDATETIME()
        );
        """,
        (
            run_number,
            submitted_by,
            application_version,
            int(asn_files),
            int(dispatch_files),
            int(sfda_rows),
        ),
    )

    return {
        "run_id": int(run_id),
        "run_number": run_number,
        "status": "Processing",
    }


def _full_sql_value(value: Any) -> Any:
    """
    Convert pandas/numpy missing values into SQL NULL.

    pymssql cannot serialize pandas.NaT and raises:
        ValueError: NaTType does not support strftime
    """
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def _full_sql_row(values: tuple) -> tuple:
    return tuple(
        _full_sql_value(value)
        for value in values
    )


def save_full_reconciliation_data(
    run_id: int,
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
    master_rows: List[Dict[str, Any]],
    replace_existing: bool = True,
    batch_size: int = 2000,
) -> Dict[str, int]:
    database = Database()
    connection = database.connect()

    try:
        cursor = connection.cursor()

        if replace_existing:
            cursor.execute(
                "DELETE FROM dbo.BatchMaster;"
            )
            cursor.execute(
                "DELETE FROM dbo.FullReceiptEvents;"
            )
            cursor.execute(
                "DELETE FROM dbo.FullDispatchEvents;"
            )

        receipt_parameters = [
            (
                int(run_id),
                row.get("Event Key"),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("Expiry Date"),
                row.get("Event Date"),
                row.get("Source File"),
                row.get("Supplier Name"),
                row.get("Supplier Code"),
                row.get("PO Number"),
                row.get("Invoice Number"),
                row.get("Inbound Shipment"),
                row.get("Trade Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item"),
                float(row.get("PackageSize") or 1),
                float(row.get("Received Quantity") or 0),
                float(row.get("Quantity Packages") or 0),
            )
            for row in receipt_rows
        ]

        dispatch_parameters = [
            (
                int(run_id),
                row.get("Event Key"),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("Expiry Date"),
                row.get("Event Date"),
                row.get("Source File"),
                row.get("To Address"),
                row.get("Sales Order Number"),
                row.get("Order Line"),
                row.get("Trade Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item Number"),
                float(row.get("PackageSize") or 1),
                float(row.get("Dispatched Quantity") or 0),
                float(row.get("Quantity Packages") or 0),
            )
            for row in dispatch_rows
        ]

        master_parameters = [
            (
                int(run_id),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("GTIN"),
                row.get("Drug Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item Number"),
                float(row.get("PackageSize") or 1),
                row.get("WMS Receipt Expiry Date"),
                row.get("WMS Dispatch Expiry Date"),
                row.get("SFDA Expiry Date"),
                row.get("First Receipt Date"),
                row.get("Last Receipt Date"),
                row.get("First Dispatch Date"),
                row.get("Last Dispatch Date"),
                float(row.get("Total Received Units") or 0),
                float(row.get("Total Received Packages") or 0),
                float(row.get("Total Dispatched Units") or 0),
                float(row.get("Total Dispatched Packages") or 0),
                float(row.get("Net Physical Packages") or 0),
                float(row.get("SFDA Quantity") or 0),
                float(row.get("SFDA Active") or 0),
                float(row.get("SFDA Receive Pending") or 0),
                float(row.get("SFDA Send Pending") or 0),
                float(
                    row.get(
                        "Physical vs SFDA Active Variance"
                    )
                    or 0
                ),
                float(
                    row.get(
                        "Historical Receipt Uncovered"
                    )
                    or 0
                ),
                float(
                    row.get(
                        "Historical Dispatch Uncovered"
                    )
                    or 0
                ),
                row.get("Master Status"),
            )
            for row in master_rows
        ]

        def insert_batches(sql: str, rows: List[tuple]):
            clean_rows = [
                _full_sql_row(row)
                for row in rows
            ]

            for start in range(
                0,
                len(clean_rows),
                max(1, int(batch_size)),
            ):
                cursor.executemany(
                    sql,
                    clean_rows[
                        start:
                        start + batch_size
                    ],
                )

        insert_batches(
            """
            INSERT INTO dbo.FullReceiptEvents
            (
                FullRunID, EventKey, BN,
                ExpiryMonthKey, WMSExpiryDate,
                EventDate, SourceFile,
                SupplierName, SupplierCode,
                PONumber, InvoiceNumber,
                InboundShipment, TradeName,
                GenericItemNumber,
                TradeItemNumber, PackageSize,
                QuantityUnits, QuantityPackages
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            );
            """,
            receipt_parameters,
        )

        insert_batches(
            """
            INSERT INTO dbo.FullDispatchEvents
            (
                FullRunID, EventKey, BN,
                ExpiryMonthKey, WMSExpiryDate,
                EventDate, SourceFile,
                CustomerName, SalesOrderNumber,
                OrderLine, TradeName,
                GenericItemNumber,
                TradeItemNumber, PackageSize,
                QuantityUnits, QuantityPackages
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            );
            """,
            dispatch_parameters,
        )

        insert_batches(
            """
            INSERT INTO dbo.BatchMaster
            (
                FullRunID, BN, ExpiryMonthKey,
                GTIN, DrugName,
                GenericItemNumber,
                TradeItemNumber, PackageSize,
                WMSReceiptExpiryDate,
                WMSDispatchExpiryDate,
                SFDAExpiryDate,
                FirstReceiptDate,
                LastReceiptDate,
                FirstDispatchDate,
                LastDispatchDate,
                TotalReceivedUnits,
                TotalReceivedPackages,
                TotalDispatchedUnits,
                TotalDispatchedPackages,
                NetPhysicalPackages,
                SFDAQuantity, SFDAActive,
                SFDAReceivePending,
                SFDASendPending,
                PhysicalActiveVariance,
                HistoricalReceiptUncovered,
                HistoricalDispatchUncovered,
                MasterStatus
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            );
            """,
            master_parameters,
        )

        connection.commit()

        return {
            "receipt_events":
                len(receipt_parameters),
            "dispatch_events":
                len(dispatch_parameters),
            "master_records":
                len(master_parameters),
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def complete_full_reconciliation_run(
    run_id: int,
    receipt_events: int,
    dispatch_events: int,
    master_records: int,
    balanced_records: int,
    review_records: int,
    missing_sfda_records: int,
) -> None:
    database = Database()

    database.execute(
        """
        UPDATE dbo.FullReconciliationRuns
        SET
            ReceiptEvents = %s,
            DispatchEvents = %s,
            MasterRecords = %s,
            BalancedRecords = %s,
            ReviewRecords = %s,
            MissingSFDARecords = %s,
            Status = N'Completed',
            ErrorMessage = NULL,
            CompletedAt = SYSUTCDATETIME()
        WHERE FullRunID = %s;
        """,
        (
            int(receipt_events),
            int(dispatch_events),
            int(master_records),
            int(balanced_records),
            int(review_records),
            int(missing_sfda_records),
            int(run_id),
        ),
    )


def fail_full_reconciliation_run(
    run_id: int,
    error_message: str,
) -> None:
    database = Database()

    database.execute(
        """
        UPDATE dbo.FullReconciliationRuns
        SET
            Status = N'Failed',
            ErrorMessage = %s,
            CompletedAt = SYSUTCDATETIME()
        WHERE FullRunID = %s;
        """,
        (
            str(error_message)[:4000],
            int(run_id),
        ),
    )


def get_batch_master(
    limit: int = 500,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    initialize_full_reconciliation_database()

    database = Database()
    safe_limit = max(
        1,
        min(int(limit), 5000),
    )

    if status:
        return database.fetch_all(
            f"""
            SELECT TOP ({safe_limit}) *
            FROM dbo.BatchMaster
            WHERE MasterStatus = %s
            ORDER BY
                ExpiryMonthKey,
                BN;
            """,
            (status,),
        )

    return database.fetch_all(
        f"""
        SELECT TOP ({safe_limit}) *
        FROM dbo.BatchMaster
        ORDER BY
            CASE MasterStatus
                WHEN N'REVIEW REQUIRED'
                    THEN 1
                WHEN N'NOT IN SFDA'
                    THEN 2
                ELSE 3
            END,
            ExpiryMonthKey,
            BN;
        """
    )


# =====================================================================
# Incremental Full Reconciliation
# =====================================================================

def ensure_full_reconciliation_incremental_indexes() -> None:
    database = Database()

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'UX_FullReceiptEvents_EventKey'
              AND object_id = OBJECT_ID(N'dbo.FullReceiptEvents')
        )
        BEGIN
            CREATE UNIQUE INDEX UX_FullReceiptEvents_EventKey
            ON dbo.FullReceiptEvents(EventKey);
        END;
        """
    )

    database.execute(
        """
        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = N'UX_FullDispatchEvents_EventKey'
              AND object_id = OBJECT_ID(N'dbo.FullDispatchEvents')
        )
        BEGIN
            CREATE UNIQUE INDEX UX_FullDispatchEvents_EventKey
            ON dbo.FullDispatchEvents(EventKey);
        END;
        """
    )


def clear_full_reconciliation_history() -> None:
    database = Database()
    connection = database.connect()

    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dbo.BatchMaster;")
        cursor.execute("DELETE FROM dbo.FullReceiptEvents;")
        cursor.execute("DELETE FROM dbo.FullDispatchEvents;")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _existing_event_keys(
    table_name: str,
    event_keys: List[str],
    batch_size: int = 1000,
) -> set:
    if not event_keys:
        return set()

    allowed_tables = {
        "FullReceiptEvents",
        "FullDispatchEvents",
    }
    if table_name not in allowed_tables:
        raise ValueError("Unsupported event table.")

    database = Database()
    existing = set()

    for start in range(0, len(event_keys), batch_size):
        chunk = event_keys[start:start + batch_size]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = database.fetch_all(
            f"""
            SELECT EventKey
            FROM dbo.{table_name}
            WHERE EventKey IN ({placeholders});
            """,
            tuple(chunk),
        )
        existing.update(
            str(row["EventKey"])
            for row in rows
            if row.get("EventKey")
        )

    return existing


def _deduplicate_full_event_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Remove duplicate Event Keys inside the same uploaded run.

    SQL unique indexes remain enabled as the final database safeguard.
    The first occurrence is retained because rows sharing the same
    Event Key represent the same historical transaction.
    """
    unique_rows: List[Dict[str, Any]] = []
    seen_keys = set()
    duplicate_count = 0

    for row in rows:
        event_key = str(
            row.get("Event Key")
            or ""
        ).strip()

        if not event_key:
            continue

        if event_key in seen_keys:
            duplicate_count += 1
            continue

        seen_keys.add(event_key)
        unique_rows.append(row)

    return unique_rows, duplicate_count


def append_full_reconciliation_events(
    run_id: int,
    receipt_rows: List[Dict[str, Any]],
    dispatch_rows: List[Dict[str, Any]],
    batch_size: int = 2000,
) -> Dict[str, int]:
    initialize_full_reconciliation_database()
    ensure_full_reconciliation_incremental_indexes()

    (
        unique_receipt_rows,
        receipt_run_duplicates,
    ) = _deduplicate_full_event_rows(
        receipt_rows
    )

    (
        unique_dispatch_rows,
        dispatch_run_duplicates,
    ) = _deduplicate_full_event_rows(
        dispatch_rows
    )

    receipt_keys = [
        str(row.get("Event Key") or "").strip()
        for row in unique_receipt_rows
    ]
    dispatch_keys = [
        str(row.get("Event Key") or "").strip()
        for row in unique_dispatch_rows
    ]

    existing_receipt = _existing_event_keys(
        "FullReceiptEvents",
        receipt_keys,
    )
    existing_dispatch = _existing_event_keys(
        "FullDispatchEvents",
        dispatch_keys,
    )

    new_receipt_rows = [
        row
        for row in unique_receipt_rows
        if str(
            row.get("Event Key")
            or ""
        ).strip() not in existing_receipt
    ]
    new_dispatch_rows = [
        row
        for row in unique_dispatch_rows
        if str(
            row.get("Event Key")
            or ""
        ).strip() not in existing_dispatch
    ]

    receipt_existing_duplicates = (
        len(unique_receipt_rows)
        - len(new_receipt_rows)
    )
    dispatch_existing_duplicates = (
        len(unique_dispatch_rows)
        - len(new_dispatch_rows)
    )

    database = Database()
    connection = database.connect()

    try:
        cursor = connection.cursor()

        receipt_parameters = [
            _full_sql_row((
                int(run_id),
                row.get("Event Key"),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("Expiry Date"),
                row.get("Event Date"),
                row.get("Source File"),
                row.get("Supplier Name"),
                row.get("Supplier Code"),
                row.get("PO Number"),
                row.get("Invoice Number"),
                row.get("Inbound Shipment"),
                row.get("Trade Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item"),
                float(row.get("PackageSize") or 1),
                float(row.get("Received Quantity") or 0),
                float(row.get("Quantity Packages") or 0),
            ))
            for row in new_receipt_rows
        ]

        dispatch_parameters = [
            _full_sql_row((
                int(run_id),
                row.get("Event Key"),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("Expiry Date"),
                row.get("Event Date"),
                row.get("Source File"),
                row.get("To Address"),
                row.get("Sales Order Number"),
                row.get("Order Line"),
                row.get("Trade Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item Number"),
                float(row.get("PackageSize") or 1),
                float(row.get("Dispatched Quantity") or 0),
                float(row.get("Quantity Packages") or 0),
            ))
            for row in new_dispatch_rows
        ]

        def insert_batches(sql: str, rows: List[tuple]):
            for start in range(0, len(rows), max(1, int(batch_size))):
                cursor.executemany(
                    sql,
                    rows[start:start + batch_size],
                )

        insert_batches(
            """
            INSERT INTO dbo.FullReceiptEvents
            (
                FullRunID, EventKey, BN,
                ExpiryMonthKey, WMSExpiryDate,
                EventDate, SourceFile,
                SupplierName, SupplierCode,
                PONumber, InvoiceNumber,
                InboundShipment, TradeName,
                GenericItemNumber,
                TradeItemNumber, PackageSize,
                QuantityUnits, QuantityPackages
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            );
            """,
            receipt_parameters,
        )

        insert_batches(
            """
            INSERT INTO dbo.FullDispatchEvents
            (
                FullRunID, EventKey, BN,
                ExpiryMonthKey, WMSExpiryDate,
                EventDate, SourceFile,
                CustomerName, SalesOrderNumber,
                OrderLine, TradeName,
                GenericItemNumber,
                TradeItemNumber, PackageSize,
                QuantityUnits, QuantityPackages
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            );
            """,
            dispatch_parameters,
        )

        connection.commit()

        return {
            "new_receipt_events":
                len(new_receipt_rows),
            "skipped_receipt_events":
                receipt_run_duplicates
                + receipt_existing_duplicates,
            "receipt_duplicates_in_upload":
                receipt_run_duplicates,
            "receipt_duplicates_in_database":
                receipt_existing_duplicates,
            "new_dispatch_events":
                len(new_dispatch_rows),
            "skipped_dispatch_events":
                dispatch_run_duplicates
                + dispatch_existing_duplicates,
            "dispatch_duplicates_in_upload":
                dispatch_run_duplicates,
            "dispatch_duplicates_in_database":
                dispatch_existing_duplicates,
        }

    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def get_full_reconciliation_summaries() -> Dict[str, List[Dict[str, Any]]]:
    database = Database()

    receipt_summary = database.fetch_all(
        """
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            SUM(QuantityUnits) AS [Total Received Units],
            SUM(QuantityPackages) AS [Total Received Packages],
            MIN(EventDate) AS [First Receipt Date],
            MAX(EventDate) AS [Last Receipt Date],
            MAX(TradeName) AS [Receipt Trade Name],
            MAX(GenericItemNumber) AS [Generic Item Number],
            MAX(TradeItemNumber) AS [Receipt Trade Item Number],
            MAX(WMSExpiryDate) AS [WMS Receipt Expiry Date],
            MAX(PackageSize) AS [Receipt PackageSize]
        FROM dbo.FullReceiptEvents
        GROUP BY BN, ExpiryMonthKey;
        """
    )

    dispatch_summary = database.fetch_all(
        """
        SELECT
            BN,
            ExpiryMonthKey AS [Expiry Month Key],
            SUM(QuantityUnits) AS [Total Dispatched Units],
            SUM(QuantityPackages) AS [Total Dispatched Packages],
            MIN(EventDate) AS [First Dispatch Date],
            MAX(EventDate) AS [Last Dispatch Date],
            MAX(TradeName) AS [Dispatch Trade Name],
            MAX(GenericItemNumber) AS [Dispatch Generic Item Number],
            MAX(TradeItemNumber) AS [Dispatch Trade Item Number],
            MAX(WMSExpiryDate) AS [WMS Dispatch Expiry Date],
            MAX(PackageSize) AS [Dispatch PackageSize]
        FROM dbo.FullDispatchEvents
        GROUP BY BN, ExpiryMonthKey;
        """
    )

    return {
        "receipt_summary": receipt_summary,
        "dispatch_summary": dispatch_summary,
    }


def replace_batch_master(
    run_id: int,
    master_rows: List[Dict[str, Any]],
    batch_size: int = 1000,
) -> None:
    database = Database()
    connection = database.connect()

    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dbo.BatchMaster;")

        parameters = [
            _full_sql_row((
                int(run_id),
                row.get("BN"),
                row.get("Expiry Month Key"),
                row.get("GTIN"),
                row.get("Drug Name"),
                row.get("Generic Item Number"),
                row.get("Trade Item Number"),
                float(row.get("PackageSize") or 1),
                row.get("WMS Receipt Expiry Date"),
                row.get("WMS Dispatch Expiry Date"),
                row.get("SFDA Expiry Date"),
                row.get("First Receipt Date"),
                row.get("Last Receipt Date"),
                row.get("First Dispatch Date"),
                row.get("Last Dispatch Date"),
                float(row.get("Total Received Units") or 0),
                float(row.get("Total Received Packages") or 0),
                float(row.get("Total Dispatched Units") or 0),
                float(row.get("Total Dispatched Packages") or 0),
                float(row.get("Net Physical Packages") or 0),
                float(row.get("SFDA Quantity") or 0),
                float(row.get("SFDA Active") or 0),
                float(row.get("SFDA Receive Pending") or 0),
                float(row.get("SFDA Send Pending") or 0),
                float(row.get("Physical vs SFDA Active Variance") or 0),
                float(row.get("Historical Receipt Uncovered") or 0),
                float(row.get("Historical Dispatch Uncovered") or 0),
                row.get("Master Status"),
            ))
            for row in master_rows
        ]

        sql = """
        INSERT INTO dbo.BatchMaster
        (
            FullRunID, BN, ExpiryMonthKey,
            GTIN, DrugName,
            GenericItemNumber,
            TradeItemNumber, PackageSize,
            WMSReceiptExpiryDate,
            WMSDispatchExpiryDate,
            SFDAExpiryDate,
            FirstReceiptDate,
            LastReceiptDate,
            FirstDispatchDate,
            LastDispatchDate,
            TotalReceivedUnits,
            TotalReceivedPackages,
            TotalDispatchedUnits,
            TotalDispatchedPackages,
            NetPhysicalPackages,
            SFDAQuantity, SFDAActive,
            SFDAReceivePending,
            SFDASendPending,
            PhysicalActiveVariance,
            HistoricalReceiptUncovered,
            HistoricalDispatchUncovered,
            MasterStatus
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        );
        """

        for start in range(0, len(parameters), batch_size):
            cursor.executemany(
                sql,
                parameters[start:start + batch_size],
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
