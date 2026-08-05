import pandas as pd


class Normalizer:
    @staticmethod
    def _find_column(df, candidates):
        normalized = {
            str(column).strip().lower(): column
            for column in df.columns
        }

        for candidate in candidates:
            match = normalized.get(
                str(candidate).strip().lower()
            )
            if match is not None:
                return match

        return None

    @staticmethod
    def _required_series(df, candidates):
        column = Normalizer._find_column(
            df,
            candidates,
        )

        if column is None:
            raise ValueError(
                f"Required source column not found. "
                f"Expected one of: {candidates}"
            )

        return df[column]

    @staticmethod
    def _optional_series(
        df,
        candidates,
        default="",
    ):
        column = Normalizer._find_column(
            df,
            candidates,
        )

        if column is None:
            return pd.Series(
                [default] * len(df),
                index=df.index,
                dtype=object,
            )

        return df[column]

    @staticmethod
    def text(series):
        return (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )

    @staticmethod
    def identifier(series, length=None):
        result = (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\s+", "", regex=True)
        )

        if length is not None:
            result = result.str.zfill(length)

        return result

    @staticmethod
    def _safe_datetime(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        """Parse dates safely while respecting each report's source format.

        Values already loaded by Excel as real date/datetime objects are
        preserved. Text values are first parsed using the supplied explicit
        formats, then a conservative mixed-format fallback is applied only to
        values that remain unresolved.
        """

        cleaned = series.copy()

        text_values = (
            cleaned.fillna("")
            .astype(str)
            .str.strip()
        )

        invalid_date_mask = (
            text_values.str.match(
                r"^9999[-/]",
                na=False,
            )
            | text_values.str.contains(
                r"[-/]9999(?:\s|$)",
                regex=True,
                na=False,
            )
        )

        cleaned = cleaned.mask(
            invalid_date_mask,
            None,
        )

        # Preserve values already interpreted by Excel/pandas as datetimes.
        result = pd.to_datetime(
            cleaned.where(
                cleaned.map(
                    lambda value: isinstance(
                        value,
                        (
                            pd.Timestamp,
                            __import__("datetime").datetime,
                            __import__("datetime").date,
                        ),
                    )
                )
            ),
            errors="coerce",
        )

        unresolved = result.isna() & cleaned.notna()

        for date_format in formats or []:
            if not unresolved.any():
                break

            parsed = pd.to_datetime(
                cleaned.where(unresolved),
                errors="coerce",
                format=date_format,
            )
            result = result.fillna(parsed)
            unresolved = result.isna() & cleaned.notna()

        if unresolved.any():
            parsed = pd.to_datetime(
                cleaned.where(unresolved),
                errors="coerce",
                dayfirst=fallback_dayfirst,
                format="mixed",
            )
            result = result.fillna(parsed)

        result = result.mask(
            (result > pd.Timestamp.max)
            | (result < pd.Timestamp.min),
            pd.NaT,
        )

        return result.astype(
            "datetime64[ns]"
        )

    @staticmethod
    def date(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        return (
            Normalizer._safe_datetime(
                series,
                formats=formats,
                fallback_dayfirst=fallback_dayfirst,
            )
            .dt.normalize()
        )

    @staticmethod
    def datetime(
        series,
        formats=None,
        fallback_dayfirst=True,
    ):
        return Normalizer._safe_datetime(
            series,
            formats=formats,
            fallback_dayfirst=fallback_dayfirst,
        )

    @staticmethod
    def number(series):
        return (
            pd.to_numeric(
                series,
                errors="coerce",
            )
            .fillna(0)
        )

    @staticmethod
    def normalize_asn(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Batch Number", "BN", "Batch/Lot"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiration Date", "Expiry Date", "Best Before Date"],
            ),
            formats=[
                "%d/%m/%Y",
                "%d/%m/%Y %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Received Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Received Qty", "Received Quantity"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Description", "Trade Name"],
            )
        )
        df["Trade Item"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item", "Trade Item Number"],
            )
        )
        df["Inbound Shipment"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Inbound Shipment", "Shipment Reference"],
            )
        )
        df["ASN Line"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["ASN Line", "Line Number"],
            )
        )
        df["Supplier Name"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                ["Supplier Name", "Vendor Name"],
            )
        )
        df["Supplier Code"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Supplier Code", "Vendor Code"],
            )
        )
        df["Description"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                [
                    "Description",
                    "Item Description",
                    "Generic Description",
                ],
            )
        )
        df["Item Family Group"] = Normalizer.text(
            Normalizer._optional_series(
                df,
                [
                    "Item Family Group",
                    "Item Family",
                    "Family Group",
                ],
            )
        )
        df["Received Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Received Date",
                    "Receipt Date",
                    "Actual Receipt Date",
                    "Date Received",
                    "ASN Closed Date",
                    "Closed Date",
                    "Transaction Date",
                ],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Generic Item Number",
                    "Item Number",
                    "Generic Number",
                ],
            )
        )

        return df

    @staticmethod
    def normalize_inventory(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Lot No/Batch", "BN", "Batch Number"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiry Date", "Expiration Date"],
            )
        )
        df["Available Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Available Qty", "Available Quantity"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Item Description", "Trade Name", "Trade Description"],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Generic Item Number", "Item Number"],
            )
        )
        df["Trade Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item Number", "Trade Item"],
            )
        )

        return df

    @staticmethod
    def normalize_dispatch(df):
        df = df.copy()

        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Batch/Lot", "BN", "Batch Number"],
            )
        )
        # Full Dispatch expiry is sourced primarily from "Best Before Date".
        # The fallback names are retained only for backward compatibility.
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Best Before Date", "Expiry Date", "Expiration Date"],
            ),
            formats=[
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Dispatched Quantity"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["Pick Qty", "Dispatched Quantity", "Dispatch Qty"],
            )
        )
        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Description", "Trade Name"],
            )
        )
        df["Trade Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Trade Item Number", "Trade Item"],
            )
        )
        df["To Address"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["To Address", "Customer Name"],
            )
        )
        df["Sales Order Number"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["Sales Order Number", "Sales Order"],
            )
        )
        df["Order Line"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["order line", "Order Line", "Line Number"],
            )
        )
        df["Dispatch Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Confirm Date",
                    "Confirmed Date",
                    "Confirmation Date",
                    "Dispatched Date",
                    "Dispatch Date",
                    "Actual Dispatch Date",
                    "Ship Date",
                    "Shipment Date",
                    "Date Dispatched",
                    "Transaction Date",
                ],
            )
        )
        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                ["Generic Item Number", "Item Number"],
            )
        )

        return df

    @staticmethod
    def normalize_sfda(df):
        df = df.copy()

        df["GTIN"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["GTIN"],
            ),
            length=14,
        )
        df["BN"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["BN", "Batch Number"],
            )
        )
        df["Expiry Date"] = Normalizer.date(
            Normalizer._required_series(
                df,
                ["Expiry Date", "Expiration Date"],
            ),
            formats=[
                "%d-%m-%Y",
                "%d-%m-%Y %H:%M:%S",
            ],
            fallback_dayfirst=True,
        )
        df["Drug Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Drug Name"],
            )
        )

        for target, candidates in {
            "Quantity": ["Quantity"],
            "Active": ["Active"],
            "Quantity Receive Pending": [
                "Quantity Receive Pending",
                "Quantity ReceivePending",
            ],
            "Quantity sent pending": [
                "Quantity sent pending",
                "Quantity Send Pending",
            ],
        }.items():
            df[target] = Normalizer.number(
                Normalizer._required_series(
                    df,
                    candidates,
                )
            )

        return df

    @staticmethod
    def normalize_packsize(df):
        df = df.copy()

        df["Trade Name"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["Trade Name"],
            )
        )
        df["PackageSize"] = Normalizer.number(
            Normalizer._required_series(
                df,
                ["PackageSize", "Package Size"],
            )
        )

        return df

    @staticmethod
    def normalize_gln(df):
        df = df.copy()

        df["To Address"] = Normalizer.text(
            Normalizer._required_series(
                df,
                ["To Address"],
            )
        )
        df["GLN"] = Normalizer.identifier(
            Normalizer._required_series(
                df,
                ["GLN"],
            )
        )

        df = df[
            (df["To Address"] != "")
            & (df["GLN"] != "")
        ].copy()

        return (
            df.drop_duplicates(
                subset=["To Address"],
                keep="first",
            )
            .reset_index(drop=True)
        )
