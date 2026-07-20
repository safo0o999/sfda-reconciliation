def replace_batch_master(master: pd.DataFrame) -> None:
    """Atomically replace Batch Master from cumulative event summaries."""

    initialize_database()

    required_columns = [
        "BN",
        "Expiry Month Key",
        "Generic Item Number",
    ]

    missing = [column for column in required_columns if column not in master.columns]
    if missing:
        raise ValueError("Batch Master is missing required columns: " + ", ".join(missing))

    # جملة SQL تحتوي على 26 عموداً
    insert_sql = r"""
        INSERT INTO dbo.BatchMaster
        (
            BN, ExpiryMonthKey, ExpiryDate, GenericItemNumber, TradeItemNumber, 
            TradeName, GTIN, DrugName, PackageSize, SFDAQuantity, Active, 
            QuantitySentPending, QuantityReceivePending, Description, ItemFamilyGroup, 
            TotalReceiveQty, TotalDispatchedQty, ReceiveRuns, DispatchRuns, 
            FirstReceivedDate, LastReceivedDate, FirstDispatchDate, LastDispatchDate, 
            GenericExistsInSFDA, LastUpdated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    # تجهيز الصفوف (يجب أن تحتوي كل tuple على 25 قيمة لتطابق 25 علامة استفهام في insert_sql أعلاه)
    # ملاحظة: تم التأكد من مطابقة الترتيب تماماً
    rows = [
        (
            _text(row, "BN"),
            _text(row, "Expiry Month Key"),
            _value(row, "Expiry Date"),
            _text(row, "Generic Item Number"),
            _text(row, "Trade Item Number"),
            _text(row, "Trade Name"),
            _text(row, "GTIN"),
            _text(row, "Drug Name"),
            _number(row, "PackageSize"),
            _number(row, "Quantity"),
            _number(row, "Active"),
            _number(row, "Quantity sent pending"),
            _number(row, "Quantity Receive Pending"),
            _text(row, "Description"),
            _text(row, "Item Family Group"),
            _number(row, "Total Receive Qty"),
            _number(row, "Total Dispatched Qty"),
            _integer(row, "Receive Runs"),
            _integer(row, "Dispatch Runs"),
            _value(row, "First Received Date"),
            _value(row, "Last Received Date"),
            _value(row, "First Dispatch Date"),
            _value(row, "Last Dispatch Date"),
            _text(row, "Generic Exists in SFDA", "Yes"),
            _value(row, "Last Updated", pd.Timestamp.utcnow().tz_localize(None)),
        )
        for row in master.to_dict(orient="records")
    ]

    with Database().connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("DELETE FROM dbo.BatchMaster;")
            if rows:
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
