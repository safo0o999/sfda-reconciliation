import io
import re
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from math import floor

import pandas as pd


class Exporter:

    MAX_ROWS_PER_FILE = 20
    MAX_QUANTITY_PER_FILE = 100000
    GTIN_LENGTH = 14

    @staticmethod
    def to_csv(
        df,
        separator=",",
        include_header=True
    ):

        output = io.StringIO()

        df.to_csv(
            output,
            index=False,
            sep=separator,
            header=include_header,
            lineterminator="\r\n"
        )

        return output.getvalue()

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

            return format(value, "f").rstrip("0").rstrip(".")

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
    def build_details_file(
        df,
        file_name,
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

        for column in details.columns:

            if "Date" in str(column):

                converted_date = pd.to_datetime(
                    details[column],
                    errors="coerce"
                )

                valid_dates = (
                    converted_date.notna()
                )

                details.loc[
                    valid_dates,
                    column
                ] = converted_date.loc[
                    valid_dates
                ].dt.strftime(
                    "%d-%m-%Y"
                )

        content = Exporter.to_csv(
            df=details,
            separator=",",
            include_header=True
        )

        return {
            file_name: content
        }
