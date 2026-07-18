import os
from typing import Any, Dict, List

import pandas as pd
import pyodbc


class Database:
    def __init__(self):
        connection_string = os.getenv("SQL_CONNECTION_STRING")
        if not connection_string:
            raise RuntimeError("SQL_CONNECTION_STRING is missing.")
        self.connection_string = connection_string

    def connect(self):
        return pyodbc.connect(self.connection_string)


def initialize_database():
    sql = r"""
    IF OBJECT_ID('dbo.ReceiptEvents','U') IS NULL
    CREATE TABLE dbo.ReceiptEvents(
        EventKey varchar(64) NOT NULL PRIMARY KEY,
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        TradeItemNumber nvarchar(120) NULL,
        TradeName nvarchar(500) NULL,
        ReceivedQuantity decimal(19,4) NOT NULL,
        InboundShipment nvarchar(150) NULL,
        ASNLine nvarchar(100) NULL,
        SupplierName nvarchar(500) NULL,
        ReceivedDate datetime2 NULL,
        CreatedAt datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    IF OBJECT_ID('dbo.DispatchEvents','U') IS NULL
    CREATE TABLE dbo.DispatchEvents(
        EventKey varchar(64) NOT NULL PRIMARY KEY,
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        TradeItemNumber nvarchar(120) NULL,
        TradeName nvarchar(500) NULL,
        DispatchedQuantity decimal(19,4) NOT NULL,
        ToAddress nvarchar(500) NULL,
        SalesOrderNumber nvarchar(150) NULL,
        OrderLine nvarchar(100) NULL,
        DispatchDate datetime2 NULL,
        CreatedAt datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    IF OBJECT_ID('dbo.BatchMaster','U') IS NULL
    CREATE TABLE dbo.BatchMaster(
        BN nvarchar(120) NOT NULL,
        ExpiryMonthKey char(7) NOT NULL,
        ExpiryDate date NULL,
        GenericItemNumber nvarchar(120) NOT NULL,
        GTIN nvarchar(20) NULL,
        DrugName nvarchar(500) NULL,
        TotalReceiveQty decimal(19,4) NOT NULL DEFAULT 0,
        TotalDispatchedQty decimal(19,4) NOT NULL DEFAULT 0,
        ReceiveRuns int NOT NULL DEFAULT 0,
        DispatchRuns int NOT NULL DEFAULT 0,
        FirstReceivedDate datetime2 NULL,
        LastReceivedDate datetime2 NULL,
        FirstDispatchDate datetime2 NULL,
        LastDispatchDate datetime2 NULL,
        GenericExistsInSFDA varchar(3) NOT NULL DEFAULT 'Yes',
        LastUpdated datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_BatchMaster PRIMARY KEY(BN, ExpiryMonthKey, GenericItemNumber)
    );
    """
    with Database().connect() as connection:
        connection.cursor().execute(sql)
        connection.commit()


def _value(row, name, default=None):
    value = row.get(name, default)
    try:
        return default if pd.isna(value) else value
    except Exception:
        return value


def append_events(receipt_rows: List[Dict[str, Any]], dispatch_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    initialize_database()
    receipt_sql = """
    IF NOT EXISTS(SELECT 1 FROM dbo.ReceiptEvents WHERE EventKey=?)
    INSERT INTO dbo.ReceiptEvents(EventKey,BN,ExpiryMonthKey,ExpiryDate,GenericItemNumber,TradeItemNumber,TradeName,ReceivedQuantity,InboundShipment,ASNLine,SupplierName,ReceivedDate)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """
    dispatch_sql = """
    IF NOT EXISTS(SELECT 1 FROM dbo.DispatchEvents WHERE EventKey=?)
    INSERT INTO dbo.DispatchEvents(EventKey,BN,ExpiryMonthKey,ExpiryDate,GenericItemNumber,TradeItemNumber,TradeName,DispatchedQuantity,ToAddress,SalesOrderNumber,OrderLine,DispatchDate)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """
    inserted_receipts = inserted_dispatches = 0
    with Database().connect() as connection:
        cursor = connection.cursor()
        for row in receipt_rows:
            before = cursor.rowcount
            cursor.execute(receipt_sql, (
                _value(row,"Event Key"), _value(row,"BN",""), _value(row,"Expiry Month Key",""),
                _value(row,"Expiry Date"), _value(row,"Generic Item Number",""), _value(row,"Trade Item",""),
                _value(row,"Trade Name",""), float(_value(row,"Received Quantity",0) or 0),
                _value(row,"Inbound Shipment",""), _value(row,"ASN Line",""), _value(row,"Supplier Name",""),
                _value(row,"Received Date")
            ))
            inserted_receipts += max(cursor.rowcount, 0)
        for row in dispatch_rows:
            cursor.execute(dispatch_sql, (
                _value(row,"Event Key"), _value(row,"BN",""), _value(row,"Expiry Month Key",""),
                _value(row,"Expiry Date"), _value(row,"Generic Item Number",""), _value(row,"Trade Item Number",""),
                _value(row,"Trade Name",""), float(_value(row,"Dispatched Quantity",0) or 0),
                _value(row,"To Address",""), _value(row,"Sales Order Number",""), _value(row,"Order Line",""),
                _value(row,"Dispatch Date")
            ))
            inserted_dispatches += max(cursor.rowcount, 0)
        connection.commit()
    return {"receipt_events": inserted_receipts, "dispatch_events": inserted_dispatches}


def get_event_summaries():
    with Database().connect() as connection:
        receipt = pd.read_sql("""
            SELECT BN, ExpiryMonthKey AS [Expiry Month Key], GenericItemNumber AS [Generic Item Number],
                   COUNT(*) AS [Receive Runs], SUM(ReceivedQuantity) AS [Total Receive Qty],
                   MIN(ReceivedDate) AS [First Received Date], MAX(ReceivedDate) AS [Last Received Date]
            FROM dbo.ReceiptEvents GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        """, connection)
        dispatch = pd.read_sql("""
            SELECT BN, ExpiryMonthKey AS [Expiry Month Key], GenericItemNumber AS [Generic Item Number],
                   COUNT(*) AS [Dispatch Runs], SUM(DispatchedQuantity) AS [Total Dispatched Qty],
                   MIN(DispatchDate) AS [First Dispatch Date], MAX(DispatchDate) AS [Last Dispatch Date]
            FROM dbo.DispatchEvents GROUP BY BN, ExpiryMonthKey, GenericItemNumber
        """, connection)
    return receipt, dispatch


def replace_batch_master(master: pd.DataFrame):
    initialize_database()
    with Database().connect() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dbo.BatchMaster")
        sql = """INSERT INTO dbo.BatchMaster(BN,ExpiryMonthKey,ExpiryDate,GenericItemNumber,GTIN,DrugName,TotalReceiveQty,TotalDispatchedQty,ReceiveRuns,DispatchRuns,FirstReceivedDate,LastReceivedDate,FirstDispatchDate,LastDispatchDate,GenericExistsInSFDA,LastUpdated)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        for row in master.to_dict(orient="records"):
            cursor.execute(sql, (
                _value(row,"BN",""), _value(row,"Expiry Month Key",""), _value(row,"Expiry Date"),
                _value(row,"Generic Item Number",""), _value(row,"GTIN",""), _value(row,"Drug Name",""),
                float(_value(row,"Total Receive Qty",0) or 0), float(_value(row,"Total Dispatched Qty",0) or 0),
                int(_value(row,"Receive Runs",0) or 0), int(_value(row,"Dispatch Runs",0) or 0),
                _value(row,"First Received Date"), _value(row,"Last Received Date"),
                _value(row,"First Dispatch Date"), _value(row,"Last Dispatch Date"), "Yes", _value(row,"Last Updated")
            ))
        connection.commit()


def get_batch_master_df():
    initialize_database()
    with Database().connect() as connection:
        return pd.read_sql("""
            SELECT BN, ExpiryMonthKey AS [Expiry Month Key], ExpiryDate AS [Expiry Date],
                   GenericItemNumber AS [Generic Item Number], GTIN, DrugName AS [Drug Name],
                   TotalReceiveQty AS [Total Receive Qty], TotalDispatchedQty AS [Total Dispatched Qty],
                   ReceiveRuns AS [Receive Runs], DispatchRuns AS [Dispatch Runs],
                   FirstReceivedDate AS [First Received Date], LastReceivedDate AS [Last Received Date],
                   FirstDispatchDate AS [First Dispatch Date], LastDispatchDate AS [Last Dispatch Date],
                   GenericExistsInSFDA AS [Generic Exists in SFDA], LastUpdated AS [Last Updated]
            FROM dbo.BatchMaster
        """, connection)


def get_dispatch_events_df():
    initialize_database()
    with Database().connect() as connection:
        return pd.read_sql("""
            SELECT BN, ExpiryMonthKey AS [Expiry Month Key], ExpiryDate AS [Expiry Date],
                   GenericItemNumber AS [Generic Item Number], TradeItemNumber AS [Trade Item Number],
                   TradeName AS [Trade Name], DispatchedQuantity AS [Dispatched Quantity],
                   ToAddress AS [To Address], SalesOrderNumber AS [Sales Order Number],
                   OrderLine AS [Order Line], DispatchDate AS [Dispatch Date]
            FROM dbo.DispatchEvents ORDER BY DispatchDate, SalesOrderNumber, OrderLine
        """, connection)


def test_database_connection():
    initialize_database()
    with Database().connect() as connection:
        row = connection.cursor().execute("SELECT DB_NAME(), @@SERVERNAME, SYSUTCDATETIME()").fetchone()
    return {"status":"Connected", "database":row[0], "server":row[1], "server_utc_time":row[2]}
