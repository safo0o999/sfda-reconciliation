import io
from math import floor

import pandas as pd


class Exporter:

    MAX_ROWS_PER_FILE = 20
    MAX_QUANTITY_PER_FILE = 100000

    @staticmethod
    def to_csv(df):

        output = io.StringIO()

        df.to_csv(
            output,
            index=False
        )

        return output.getvalue()

    @staticmethod
    def _normalize_gtin(value):

        if pd.isna(value):
            return ""

        value = str(value).strip()

        if value.endswith(".0"):
            value = value[:-2]

        return value

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

        return expiry.strftime("%d-%m-%Y")

    @staticmethod
    def _split_large_quantity(record, quantity_column):

        quantity = floor(
            max(
                0,
                float(record[quantity_column])
            )
        )

        split_records = []

        while quantity > 0:

            split_quantity = min(
                quantity,
                Exporter.MAX_QUANTITY_PER_FILE
            )

            split_record = record.copy()
            split_record[quantity_column] = split_quantity

            split_records.append(split_record)

            quantity -= split_quantity

        return split_records

    @staticmethod
    def _prepare_records(df, quantity_column):

        records = []

        for _, row in df.iterrows():

            quantity = pd.to_numeric(
                row.get(quantity_column),
                errors="coerce"
            )

            if pd.isna(quantity) or quantity <= 0:
                continue

            base_record = {
                "GTIN": Exporter._normalize_gtin(
                    row.get("GTIN")
                ),
                "QUANTITY": floor(float(quantity)),
                "BN": Exporter._normalize_batch(
                    row.get("BN")
                ),
                "XD": Exporter._normalize_expiry(
                    row.get("Expiry Date")
                )
            }

            if (
                not base_record["GTIN"]
                or not base_record["BN"]
                or not base_record["XD"]
            ):
                continue

            records.extend(
                Exporter._split_large_quantity(
                    base_record,
                    "QUANTITY"
                )
            )

        return records

    @staticmethod
    def _split_into_files(records):

        files = []
        current_file = []
        current_quantity = 0

        for record in records:

            quantity = int(record["QUANTITY"])

            exceeds_rows = (
                len(current_file)
                >= Exporter.MAX_ROWS_PER_FILE
            )

            exceeds_quantity = (
                current_file
                and current_quantity + quantity
                > Exporter.MAX_QUANTITY_PER_FILE
            )

            if exceeds_rows or exceeds_quantity:

                files.append(current_file)

                current_file = []
                current_quantity = 0

            current_file.append(record)
            current_quantity += quantity

        if current_file:
            files.append(current_file)

        return files

    @staticmethod
    def _build_sfda_content(records):

        lines = [
            "GTIN,QUANTITY,BN,XD"
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

        return "\n".join(lines)

    @staticmethod
    def build_sfda_upload_files(
        df,
        quantity_column,
        file_prefix
    ):

        records = Exporter._prepare_records(
            df,
            quantity_column
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
