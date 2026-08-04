import base64
import io
import re
from decimal import Decimal, InvalidOperation, ROUND_FLOOR

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo


class Exporter:
    MAX_ROWS_PER_FILE = 20
    MAX_QUANTITY_PER_FILE = 100000
    GTIN_LENGTH = 14
    DUMMY_GLN = "9999999999999"

    BATCH_MASTER_COLUMNS = [
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Quantity",
        "Active",
        "Quantity sent pending",
        "Quantity Receive Pending",
        "Generic Item Number",
        "Description",
        "Trade Description",
        "Supplier Name",
        "Supplier Code",
        "Received Quantity Each",
        "Received Quantity Pack",
        "First Received Date",
        "Last Received Date",
        "Total Dispatched Qty",
        "Total Dispatched Qty Pack",
        "First Dispatch Date",
        "Last Dispatch Date",
        "Generic Exists in SFDA",
        "Last Updated",
        "Item Family Group",
    ]

    SUPPLIER_HISTORY_COLUMNS = [
        "Supplier Name", "Supplier Code", "GTIN", "Drug Name",
        "Generic Item Number", "Description", "Trade Description", "BN",
        "Expiry Date", "PackageSize", "Received Quantity Each",
        "Received Quantity Pack", "First Received Date", "Last Received Date",
        "Item Family Group",
    ]

    CUSTOMER_HISTORY_COLUMNS = [
        "To Address", "GLN", "GTIN", "Drug Name", "Generic Item Number",
        "Trade Description", "BN", "Expiry Date", "PackageSize",
        "Dispatch Quantity Each", "Dispatch Quantity Pack",
        "First Dispatch Date", "Last Dispatch Date",
    ]

    ACCEPT_DETAILS_COLUMNS = [
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Active",
        "Quantity sent pending",
        "Quantity Receive Pending",
        "Generic Item Number",
        "Received Quantity Each",
        "Received Quantity Pack",
        "Description",
        "Inbound Shipment",
        "Supplier Name",
        "Supplier Code",
        "Item Family Group",
        "To Be Accept",
        "Processing Status",
        "Previous Run Date",
        "Previous Quantity Each",
        "Current Quantity Each",
        "Quantity Difference",
        "Package Size Status",
        "Batch Master Status",
    ]

    DISPATCH_DETAILS_COLUMNS = [
        "GTIN",
        "Drug Name",
        "BN",
        "Expiry Date",
        "PackageSize",
        "Active",
        "Quantity sent pending",
        "Quantity Receive Pending",
        "Generic Item Number",
        "Trade Name",
        "Sales Order Number",
        "Order Line",
        "To Address",
        "Dispatch Date",
        "Dispatch Quantity Each",
        "Dispatch Quantity Pack",
        "Allocated To Be Dispatch",
        "Processing Status",
        "Previous Run Date",
        "Previous Quantity Each",
        "Current Quantity Each",
        "Quantity Difference",
        "GLN",
        "Customer Status",
        "Package Size Status",
        "Batch Master Status",
    ]

    @staticmethod
    def _normalize_identifier(value):
        if pd.isna(value):
            return ""

        text = str(value).strip()

        if text.endswith(".0"):
            text = text[:-2]

        if re.fullmatch(
            r"[+-]?\d+(\.\d+)?[eE][+-]?\d+",
            text,
        ):
            try:
                text = (
                    format(Decimal(text), "f")
                    .rstrip("0")
                    .rstrip(".")
                )
            except InvalidOperation:
                pass

        return text

    @staticmethod
    def _normalize_gtin(value):
        gtin = (
            Exporter._normalize_identifier(value)
            .replace(" ", "")
        )

        if gtin.isdigit():
            gtin = gtin.zfill(
                Exporter.GTIN_LENGTH
            )

        return gtin

    @staticmethod
    def _normalize_quantity(value):
        quantity = pd.to_numeric(
            value,
            errors="coerce",
        )

        if pd.isna(quantity):
            return 0

        try:
            return max(
                0,
                int(
                    Decimal(str(quantity))
                    .to_integral_value(
                        rounding=ROUND_FLOOR
                    )
                ),
            )
        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ):
            return 0

    @staticmethod
    def _normalize_expiry(value):
        expiry = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True,
        )

        if pd.isna(expiry):
            return ""

        return expiry.strftime("%d-%m-%Y")

    @staticmethod
    def _safe_file_name(value):
        text = re.sub(
            r"[^A-Za-z0-9_-]+",
            "_",
            str(value).strip(),
        )

        return (
            text.strip("_")
            or Exporter.DUMMY_GLN
        )

    @staticmethod
    def _prepare_records(df, quantity_column):
        records = []

        if df is None or df.empty:
            return records

        for _, row in df.iterrows():
            quantity = Exporter._normalize_quantity(
                row.get(quantity_column)
            )

            if quantity <= 0:
                continue

            gtin = Exporter._normalize_gtin(
                row.get("GTIN")
            )
            batch = str(
                row.get("BN", "")
            ).strip()
            expiry = Exporter._normalize_expiry(
                row.get("Expiry Date")
            )

            if not gtin or not batch or not expiry:
                continue

            records.append({
                "GTIN": gtin,
                "QUANTITY": quantity,
                "BN": batch,
                "XD": expiry,
            })

        return records

    @staticmethod
    def _split_into_files(records):
        files = []
        current_rows = []
        current_quantity = 0

        for record in records:
            remaining = int(record["QUANTITY"])

            while remaining > 0:
                if (
                    len(current_rows)
                    >= Exporter.MAX_ROWS_PER_FILE
                    or current_quantity
                    >= Exporter.MAX_QUANTITY_PER_FILE
                ):
                    files.append(current_rows)
                    current_rows = []
                    current_quantity = 0

                available = (
                    Exporter.MAX_QUANTITY_PER_FILE
                    - current_quantity
                )

                quantity = min(
                    remaining,
                    available,
                )

                new_record = record.copy()
                new_record["QUANTITY"] = quantity

                current_rows.append(new_record)
                current_quantity += quantity
                remaining -= quantity

        if current_rows:
            files.append(current_rows)

        return files

    @staticmethod
    def _build_sfda_content(records):
        lines = ["GTIN;QUANTITY;BN;XD"]

        for record in records:
            lines.append(
                "{};{};{};{}".format(
                    record["GTIN"],
                    int(record["QUANTITY"]),
                    record["BN"],
                    record["XD"],
                )
            )

        return "\r\n".join(lines)

    @staticmethod
    def build_sfda_upload_files(
        df,
        quantity_column,
        file_prefix,
    ):
        groups = Exporter._split_into_files(
            Exporter._prepare_records(
                df,
                quantity_column,
            )
        )

        output = {}

        for index, records_group in enumerate(
            groups,
            start=1,
        ):
            file_name = (
                f"{file_prefix}_{index:03d}.csv"
            )
            output[file_name] = (
                Exporter._build_sfda_content(
                    records_group
                )
            )

        return output

    @staticmethod
    def build_dispatch_files_by_customer(
        dispatch_df,
    ):
        output = {}

        if dispatch_df is None or dispatch_df.empty:
            return output

        customer_columns = [
            "Customer Status",
            "GLN",
            "To Address",
        ]

        available_columns = [
            column
            for column in customer_columns
            if column in dispatch_df.columns
        ]

        if not available_columns:
            return output

        for group_values, customer_df in (
            dispatch_df.groupby(
                available_columns,
                dropna=False,
                sort=True,
            )
        ):
            if not isinstance(
                group_values,
                tuple,
            ):
                group_values = (group_values,)

            group_data = dict(
                zip(
                    available_columns,
                    group_values,
                )
            )

            customer_status = str(
                group_data.get(
                    "Customer Status",
                    "",
                )
            ).strip().upper()

            raw_gln = (
                Exporter._normalize_identifier(
                    group_data.get("GLN", "")
                )
            )

            is_dummy = (
                customer_status == "DUMMY"
                or not raw_gln
                or raw_gln.upper() == "DUMMY"
            )

            customer_gln = (
                Exporter.DUMMY_GLN
                if is_dummy
                else raw_gln
            )

            customer_gln = (
                Exporter._safe_file_name(
                    customer_gln
                )
            )

            groups = Exporter._split_into_files(
                Exporter._prepare_records(
                    customer_df,
                    "Allocated To Be Dispatch",
                )
            )

            for index, records_group in enumerate(
                groups,
                start=1,
            ):
                file_name = (
                    f"{customer_gln}_"
                    f"{index:03d}.csv"
                )
                output[file_name] = (
                    Exporter._build_sfda_content(
                        records_group
                    )
                )

        return output

    @staticmethod
    def _is_batch_master_report(file_name, sheet_name):
        file_text = str(file_name or "").strip().lower()
        sheet_text = str(sheet_name or "").strip().lower()
        return (
            "batch_master" in file_text
            or "batch master" in file_text
            or sheet_text == "batch master"
        )

    @staticmethod
    def _is_supplier_history_report(file_name, sheet_name):
        text = f"{file_name or ''} {sheet_name or ''}".strip().lower()
        return "supplier_history" in text or "supplier history" in text

    @staticmethod
    def _is_customer_history_report(file_name, sheet_name):
        text = f"{file_name or ''} {sheet_name or ''}".strip().lower()
        return "customer_history" in text or "customer history" in text

    @staticmethod
    def _is_accept_details_report(file_name, sheet_name):
        file_text = str(file_name or "").strip().lower()
        sheet_text = str(sheet_name or "").strip().lower()

        return (
            "accept_details" in file_text
            or "accept details" in file_text
            or sheet_text == "accept details"
        )

    @staticmethod
    def _is_dispatch_details_report(file_name, sheet_name):
        file_text = str(file_name or "").strip().lower()
        sheet_text = str(sheet_name or "").strip().lower()

        return (
            "dispatch_details" in file_text
            or "dispatch details" in file_text
            or sheet_text == "dispatch details"
        )

    @staticmethod
    def build_formatted_excel_file(
        df,
        file_name,
        sheet_name,
        title,
        columns=None,
        sort_columns=None,
    ):
        report = (
            df.copy()
            if df is not None
            else pd.DataFrame()
        )

        is_batch_master = Exporter._is_batch_master_report(
            file_name,
            sheet_name,
        )
        is_supplier_history = Exporter._is_supplier_history_report(file_name, sheet_name)
        is_customer_history = Exporter._is_customer_history_report(file_name, sheet_name)
        is_accept_details = Exporter._is_accept_details_report(
            file_name,
            sheet_name,
        )
        is_dispatch_details = Exporter._is_dispatch_details_report(
            file_name,
            sheet_name,
        )

        if is_batch_master:
            report = report.reindex(columns=Exporter.BATCH_MASTER_COLUMNS)
        elif is_supplier_history:
            report = report.reindex(columns=Exporter.SUPPLIER_HISTORY_COLUMNS)
        elif is_customer_history:
            report = report.reindex(columns=Exporter.CUSTOMER_HISTORY_COLUMNS)
        elif is_accept_details:
            report = report.reindex(
                columns=Exporter.ACCEPT_DETAILS_COLUMNS
            )
        elif is_dispatch_details:
            report = report.reindex(
                columns=Exporter.DISPATCH_DETAILS_COLUMNS
            )

        if columns and not (
            is_batch_master
            or is_accept_details
            or is_dispatch_details
        ):
            report = report[
                [
                    column
                    for column in columns
                    if column in report.columns
                ]
            ]

        if sort_columns:
            available_sort_columns = [
                column
                for column in sort_columns
                if column in report.columns
            ]

            if available_sort_columns:
                report = report.sort_values(
                    by=available_sort_columns,
                    kind="stable",
                )

        report = report.reset_index(drop=True)

        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = sheet_name[:31]

        column_count = max(
            1,
            len(report.columns),
        )
        last_column = get_column_letter(
            column_count
        )

        if is_batch_master or is_accept_details or is_dispatch_details:
            if is_batch_master:
                group_definitions = [
                    (1, 9, "SFDA Report", "5B9BD5"),
                    (10, 25, "WMS Report", "4472C4"),
                ]
            elif is_accept_details:
                group_definitions = [
                    (1, 8, "SFDA Report", "5B9BD5"),
                    (9, 16, "WMS Receiving Report", "4472C4"),
                    (17, 24, "Decision", "70AD47"),
                ]
            else:
                group_definitions = [
                    (1, 8, "SFDA Report", "5B9BD5"),
                    (9, 16, "WMS Dispatch Report", "4472C4"),
                    (17, 26, "Decision", "70AD47"),
                ]

            for start_column, end_column, group_title, fill_color in group_definitions:
                start_letter = get_column_letter(start_column)
                end_letter = get_column_letter(end_column)
                worksheet.merge_cells(
                    f"{start_letter}1:{end_letter}1"
                )
                group_cell = worksheet.cell(
                    row=1,
                    column=start_column,
                    value=group_title,
                )
                group_cell.font = Font(
                    bold=True,
                    color="FFFFFF",
                    size=11,
                )
                group_cell.fill = PatternFill(
                    fill_type="solid",
                    fgColor=fill_color,
                )
                group_cell.alignment = Alignment(
                    horizontal="center",
                    vertical="center",
                )

                for column_index in range(start_column, end_column + 1):
                    worksheet.cell(row=1, column=column_index).fill = PatternFill(
                        fill_type="solid",
                        fgColor=fill_color,
                    )

            worksheet.row_dimensions[1].height = 22
            header_row = 2
        else:
            worksheet.merge_cells(
                f"A1:{last_column}1"
            )
            worksheet["A1"] = title
            worksheet["A1"].font = Font(
                bold=True,
                color="FFFFFF",
                size=16,
            )
            worksheet["A1"].fill = PatternFill(
                fill_type="solid",
                fgColor="0F6CBD",
            )
            worksheet["A1"].alignment = Alignment(
                horizontal="center"
            )
            header_row = 3
        border = Border(
            left=Side(
                style="thin",
                color="B8C4CE",
            ),
            right=Side(
                style="thin",
                color="B8C4CE",
            ),
            top=Side(
                style="thin",
                color="B8C4CE",
            ),
            bottom=Side(
                style="thin",
                color="B8C4CE",
            ),
        )

        for column_index, column_name in enumerate(
            report.columns,
            start=1,
        ):
            cell = worksheet.cell(
                row=header_row,
                column=column_index,
                value=str(column_name),
            )
            cell.font = Font(
                bold=True,
                color="FFFFFF",
            )
            if is_batch_master:
                header_fill = "2F5597" if column_index >= 10 else "17365D"
            elif (
                is_accept_details
                or is_dispatch_details
            ) and column_index >= 17:
                header_fill = "548235"
            elif (
                is_accept_details
                or is_dispatch_details
            ) and column_index >= 9:
                header_fill = "2F5597"
            else:
                header_fill = "17365D"

            cell.fill = PatternFill(
                fill_type="solid",
                fgColor=header_fill,
            )
            cell.alignment = Alignment(
                horizontal="center",
                vertical="center",
                wrap_text=True,
            )
            cell.border = border

        for row_index, row in report.iterrows():
            excel_row = (
                header_row
                + row_index
                + 1
            )

            for column_index, column_name in enumerate(
                report.columns,
                start=1,
            ):
                value = row[column_name]
                column_text = str(
                    column_name
                ).lower()

                if pd.isna(value):
                    value = None
                elif (
                    "date" in column_text
                    or "expiry" in column_text
                ):
                    converted = pd.to_datetime(
                        value,
                        errors="coerce",
                    )
                    if not pd.isna(converted):
                        value = (
                            converted.to_pydatetime()
                        )
                elif any(
                    identifier in column_text
                    for identifier in [
                        "gtin",
                        "generic item",
                        "trade item",
                        "sales order",
                        "order number",
                        "order line",
                        "inbound shipment",
                        "gln",
                        "bn",
                    ]
                ):
                    value = (
                        Exporter._normalize_identifier(
                            value
                        )
                    )
                elif hasattr(value, "item"):
                    try:
                        value = value.item()
                    except Exception:
                        pass

                cell = worksheet.cell(
                    row=excel_row,
                    column=column_index,
                    value=value,
                )
                cell.border = border

                if any(
                    identifier in column_text
                    for identifier in [
                        "gtin",
                        "generic item",
                        "trade item",
                        "sales order",
                        "order number",
                        "order line",
                        "inbound shipment",
                        "gln",
                        "bn",
                    ]
                ):
                    cell.number_format = "@"
                elif (
                    "date" in column_text
                    or "expiry" in column_text
                ):
                    cell.number_format = (
                        "dd-mm-yyyy"
                    )
                elif isinstance(
                    value,
                    (int, float),
                ):
                    cell.number_format = "#,##0.##"

                if row_index % 2 == 1:
                    cell.fill = PatternFill(
                        fill_type="solid",
                        fgColor="EAF2F8",
                    )

        if len(report) > 0:
            report_range = (
                f"A{header_row}:"
                f"{last_column}"
                f"{header_row + len(report)}"
            )

            if is_batch_master or is_accept_details or is_dispatch_details:
                worksheet.auto_filter.ref = report_range
            else:
                table = Table(
                    displayName="ReportTable",
                    ref=report_range,
                )
                table.tableStyleInfo = (
                    TableStyleInfo(
                        name="TableStyleMedium2",
                        showRowStripes=False,
                    )
                )
                worksheet.add_table(table)

        worksheet.freeze_panes = (
            f"A{header_row + 1}"
        )
        worksheet.sheet_view.showGridLines = False

        for column_index, column_name in enumerate(
            report.columns,
            start=1,
        ):
            max_length = len(str(column_name))

            for value in report[column_name]:
                if pd.isna(value):
                    continue
                max_length = max(
                    max_length,
                    len(str(value)),
                )

            worksheet.column_dimensions[
                get_column_letter(column_index)
            ].width = min(
                max(max_length + 2, 12),
                45,
            )

        output = io.BytesIO()
        workbook.save(output)
        output.seek(0)

        content = base64.b64encode(
            output.read()
        ).decode("ascii")

        return {
            file_name: {
                "content": content,
                "encoding": "base64",
                "mime_type": (
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet"
                ),
            }
        }
