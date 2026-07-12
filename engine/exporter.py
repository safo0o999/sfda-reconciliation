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

    HEADER_FILL = "17365D"
    HEADER_FONT = "FFFFFF"
    TITLE_FILL = "0F6CBD"
    TITLE_FONT = "FFFFFF"
    ALTERNATE_FILL = "EAF2F8"
    BORDER_COLOR = "B8C4CE"

    @staticmethod
    def _normalize_identifier(value):

        if pd.isna(value):
            return ""

        if isinstance(value, bool):
            return str(value)

        if isinstance(value, int):
            return str(value)

        if isinstance(value, float):

            if pd.isna(value):
                return ""

            if value.is_integer():
                return format(value, ".0f")

            return format(
                value,
                "f"
            ).rstrip("0").rstrip(".")

        text = str(value).strip()

        if not text:
            return ""

        if re.fullmatch(
            r"[+-]?\d+(\.0+)?",
            text
        ):
            text = re.sub(
                r"\.0+$",
                "",
                text
            )

        if re.fullmatch(
            r"[+-]?\d+(\.\d+)?[eE][+-]?\d+",
            text
        ):
            try:
                text = format(
                    Decimal(text),
                    "f"
                )

                if "." in text:
                    text = (
                        text
                        .rstrip("0")
                        .rstrip(".")
                    )

            except InvalidOperation:
                pass

        return text

    @staticmethod
    def _normalize_gtin(value):

        gtin = Exporter._normalize_identifier(
            value
        )

        if not gtin:
            return ""

        gtin = gtin.replace(
            " ",
            ""
        )

        if gtin.isdigit():
            gtin = gtin.zfill(
                Exporter.GTIN_LENGTH
            )

        return gtin

    @staticmethod
    def _normalize_batch(value):

        if pd.isna(value):
            return ""

        return str(value).strip()

    @staticmethod
    def _normalize_expiry(value):

        expiry = pd.to_datetime(
            value,
            errors="coerce",
            dayfirst=True
        )

        if pd.isna(expiry):
            return ""

        return expiry.strftime(
            "%d-%m-%Y"
        )

    @staticmethod
    def _normalize_quantity(value):

        quantity = pd.to_numeric(
            value,
            errors="coerce"
        )

        if pd.isna(quantity):
            return 0

        try:
            quantity = Decimal(
                str(quantity)
            )

            quantity = quantity.to_integral_value(
                rounding=ROUND_FLOOR
            )

            return max(
                0,
                int(quantity)
            )

        except (
            InvalidOperation,
            ValueError,
            TypeError
        ):
            return 0

    @staticmethod
    def _split_large_quantity(record):

        quantity = Exporter._normalize_quantity(
            record.get("QUANTITY")
        )

        split_records = []

        while quantity > 0:

            split_quantity = min(
                quantity,
                Exporter.MAX_QUANTITY_PER_FILE
            )

            split_record = record.copy()

            split_record["QUANTITY"] = (
                split_quantity
            )

            split_records.append(
                split_record
            )

            quantity -= split_quantity

        return split_records

    @staticmethod
    def _prepare_records(
        df,
        quantity_column
    ):

        required_columns = [
            "GTIN",
            "BN",
            "Expiry Date",
            quantity_column
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing export columns: "
                f"{missing_columns}"
            )

        records = []

        for _, row in df.iterrows():

            quantity = (
                Exporter._normalize_quantity(
                    row.get(quantity_column)
                )
            )

            if quantity <= 0:
                continue

            base_record = {
                "GTIN": Exporter._normalize_gtin(
                    row.get("GTIN")
                ),
                "QUANTITY": quantity,
                "BN": Exporter._normalize_batch(
                    row.get("BN")
                ),
                "XD": Exporter._normalize_expiry(
                    row.get("Expiry Date")
                )
            }

            if not base_record["GTIN"]:
                continue

            if not base_record["BN"]:
                continue

            if not base_record["XD"]:
                continue

            records.extend(
                Exporter._split_large_quantity(
                    base_record
                )
            )

        return records

    @staticmethod
    def _split_into_files(records):

        files = []

        current_file = []
        current_total_quantity = 0

        for record in records:

            remaining_quantity = int(
                record["QUANTITY"]
            )

            while remaining_quantity > 0:

                if (
                    len(current_file)
                    >= Exporter.MAX_ROWS_PER_FILE
                    or current_total_quantity
                    >= Exporter.MAX_QUANTITY_PER_FILE
                ):

                    files.append(
                        current_file
                    )

                    current_file = []
                    current_total_quantity = 0

                available_quantity = (
                    Exporter.MAX_QUANTITY_PER_FILE
                    - current_total_quantity
                )

                quantity_for_current_file = min(
                    remaining_quantity,
                    available_quantity
                )

                file_record = record.copy()

                file_record["QUANTITY"] = (
                    quantity_for_current_file
                )

                current_file.append(
                    file_record
                )

                current_total_quantity += (
                    quantity_for_current_file
                )

                remaining_quantity -= (
                    quantity_for_current_file
                )

                if (
                    len(current_file)
                    >= Exporter.MAX_ROWS_PER_FILE
                    or current_total_quantity
                    >= Exporter.MAX_QUANTITY_PER_FILE
                ):

                    files.append(
                        current_file
                    )

                    current_file = []
                    current_total_quantity = 0

        if current_file:
            files.append(
                current_file
            )

        return files

    @staticmethod
    def _build_sfda_content(records):

        lines = [
            "GTIN;QUANTITY;BN;XD"
        ]

        for record in records:

            lines.append(
                "{};{};{};{}".format(
                    record["GTIN"],
                    int(record["QUANTITY"]),
                    record["BN"],
                    record["XD"]
                )
            )

        return "\r\n".join(
            lines
        )

    @staticmethod
    def build_sfda_upload_files(
        df,
        quantity_column,
        file_prefix
    ):

        records = Exporter._prepare_records(
            df=df,
            quantity_column=quantity_column
        )

        file_groups = Exporter._split_into_files(
            records
        )

        output_files = {}

        for index, file_records in enumerate(
            file_groups,
            start=1
        ):

            file_name = (
                f"{file_prefix}_{index:03d}.csv"
            )

            output_files[file_name] = (
                Exporter._build_sfda_content(
                    file_records
                )
            )

        return output_files

    @staticmethod
    def _clean_sheet_name(sheet_name):

        cleaned = re.sub(
            r'[:\\/*?\[\]]',
            "_",
            str(sheet_name)
        )

        return cleaned[:31] or "Report"

    @staticmethod
    def _prepare_excel_dataframe(
        df,
        columns=None,
        sort_columns=None
    ):

        details = df.copy()

        if columns:

            available_columns = [
                column
                for column in columns
                if column in details.columns
            ]

            details = details[
                available_columns
            ]

        if sort_columns:

            available_sort_columns = [
                column
                for column in sort_columns
                if column in details.columns
            ]

            if available_sort_columns:

                details = details.sort_values(
                    by=available_sort_columns,
                    kind="stable"
                )

        details = details.reset_index(
            drop=True
        )

        return details

    @staticmethod
    def _excel_value(
        value,
        column_name
    ):

        if pd.isna(value):
            return None

        column_text = str(
            column_name
        ).lower()

        identifier_columns = [
            "gtin",
            "trade item",
            "sales order",
            "order line",
            "inbound shipment",
            "asn line",
            "batch",
            "bn"
        ]

        if any(
            identifier in column_text
            for identifier in identifier_columns
        ):
            return Exporter._normalize_identifier(
                value
            )

        if "date" in column_text or "expiry" in column_text:

            converted = pd.to_datetime(
                value,
                errors="coerce"
            )

            if pd.isna(converted):
                return str(value)

            return converted.to_pydatetime()

        if isinstance(
            value,
            (
                pd.Timestamp,
            )
        ):
            return value.to_pydatetime()

        if hasattr(
            value,
            "item"
        ):
            try:
                return value.item()
            except Exception:
                pass

        return value

    @staticmethod
    def build_formatted_excel_file(
        df,
        file_name,
        sheet_name,
        title,
        columns=None,
        sort_columns=None
    ):

        details = Exporter._prepare_excel_dataframe(
            df=df,
            columns=columns,
            sort_columns=sort_columns
        )

        workbook = Workbook()

        worksheet = workbook.active

        worksheet.title = (
            Exporter._clean_sheet_name(
                sheet_name
            )
        )

        column_count = max(
            1,
            len(details.columns)
        )

        last_column = get_column_letter(
            column_count
        )

        worksheet.merge_cells(
            f"A1:{last_column}1"
        )

        title_cell = worksheet["A1"]
        title_cell.value = title
        title_cell.fill = PatternFill(
            fill_type="solid",
            fgColor=Exporter.TITLE_FILL
        )
        title_cell.font = Font(
            color=Exporter.TITLE_FONT,
            bold=True,
            size=16
        )
        title_cell.alignment = Alignment(
            horizontal="center",
            vertical="center"
        )

        worksheet.row_dimensions[1].height = 28

        header_row = 3

        thin_border = Border(
            left=Side(
                style="thin",
                color=Exporter.BORDER_COLOR
            ),
            right=Side(
                style="thin",
                color=Exporter.BORDER_COLOR
            ),
            top=Side(
                style="thin",
                color=Exporter.BORDER_COLOR
            ),
            bottom=Side(
                style="thin",
                color=Exporter.BORDER_COLOR
            )
        )

        for column_index, column_name in enumerate(
            details.columns,
            start=1
        ):

            cell = worksheet.cell(
                row=header_row,
                column=column_index,
                value=str(column_name)
            )

            cell.fill = PatternFill(
                fill_type="solid",
                fgColor=Exporter.HEADER_FILL
            )

            cell.font = Font(
                color=Exporter.HEADER_FONT,
                bold=True
            )

            cell.alignment = Alignment(
                horizontal="center",
                vertical="center"
            )

            cell.border = thin_border

        worksheet.row_dimensions[
            header_row
        ].height = 22

        for row_index, row in details.iterrows():

            excel_row = (
                header_row
                + row_index
                + 1
            )

            for column_index, column_name in enumerate(
                details.columns,
                start=1
            ):

                value = Exporter._excel_value(
                    row[column_name],
                    column_name
                )

                cell = worksheet.cell(
                    row=excel_row,
                    column=column_index,
                    value=value
                )

                cell.border = thin_border

                cell.alignment = Alignment(
                    vertical="center"
                )

                column_text = str(
                    column_name
                ).lower()

                if (
                    "gtin" in column_text
                    or "trade item" in column_text
                    or "sales order" in column_text
                    or "order line" in column_text
                    or "inbound shipment" in column_text
                    or "asn line" in column_text
                    or column_text in ["bn", "batch"]
                ):
                    cell.number_format = "@"

                elif (
                    "date" in column_text
                    or "expiry" in column_text
                ):
                    cell.number_format = "dd-mm-yyyy"

                elif isinstance(
                    value,
                    (
                        int,
                        float
                    )
                ):
                    cell.number_format = "#,##0.##"

                if row_index % 2 == 1:

                    cell.fill = PatternFill(
                        fill_type="solid",
                        fgColor=Exporter.ALTERNATE_FILL
                    )

        if len(details) > 0:

            table_reference = (
                f"A{header_row}:"
                f"{last_column}"
                f"{header_row + len(details)}"
            )

            table_name = re.sub(
                r"\W+",
                "",
                sheet_name
            )

            table_name = (
                table_name[:20]
                or "ReportTable"
            )

            table = Table(
                displayName=(
                    f"{table_name}Table"
                ),
                ref=table_reference
            )

            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=False,
                showColumnStripes=False
            )

            worksheet.add_table(
                table
            )

        worksheet.freeze_panes = (
            f"A{header_row + 1}"
        )

        worksheet.auto_filter.ref = (
            f"A{header_row}:"
            f"{last_column}"
            f"{max(header_row, header_row + len(details))}"
        )

        for column_index, column_name in enumerate(
            details.columns,
            start=1
        ):

            max_length = len(
                str(column_name)
            )

            for row_index in range(
                header_row + 1,
                header_row + len(details) + 1
            ):

                value = worksheet.cell(
                    row=row_index,
                    column=column_index
                ).value

                if value is None:
                    continue

                value_length = len(
                    str(value)
                )

                max_length = max(
                    max_length,
                    value_length
                )

            width = min(
                max(
                    max_length + 2,
                    12
                ),
                45
            )

            worksheet.column_dimensions[
                get_column_letter(
                    column_index
                )
            ].width = width

        worksheet.sheet_view.showGridLines = False

        output = io.BytesIO()

        workbook.save(
            output
        )

        output.seek(0)

        encoded_content = base64.b64encode(
            output.read()
        ).decode(
            "ascii"
        )

        return {
            file_name: {
                "content": encoded_content,
                "encoding": "base64",
                "mime_type": (
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet"
                )
            }
        }
