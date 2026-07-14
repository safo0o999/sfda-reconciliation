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
