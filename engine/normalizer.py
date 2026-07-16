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
    def _optional_series(
        df,
        candidates,
        default=""
    ):
        column = Normalizer._find_column(
            df,
            candidates
        )

        if column is None:
            return pd.Series(
                [default] * len(df),
                index=df.index,
                dtype=object
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
    def _safe_datetime(series):
        """
        Convert mixed Excel/text datetime values safely.

        Dates outside pandas datetime64[ns] range, including common
        sentinel dates such as 9999-12-30, are converted to NaT.
        The returned dtype is always datetime64[ns] so all dataframe
        merge keys use the same datetime precision.
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
                na=False
            )
            | text_values.str.contains(
                r"[-/]9999(?:\s|$)",
                regex=True,
                na=False
            )
            | text_values.isin(
                {
                    "9999-12-30",
                    "9999-12-31",
                    "30/12/9999",
                    "31/12/9999",
                    "9999/12/30",
                    "9999/12/31",
                }
            )
        )

        cleaned = cleaned.mask(
            invalid_date_mask,
            None
        )

        result = pd.to_datetime(
            cleaned,
            errors="coerce",
            dayfirst=True,
            format="mixed"
        )

        # pandas may parse large years using microsecond precision.
        # Remove anything outside datetime64[ns] before enforcing ns dtype.
        max_supported = pd.Timestamp.max
        min_supported = pd.Timestamp.min

        result = result.mask(
            (result > max_supported)
            | (result < min_supported),
            pd.NaT
        )

        return result.astype(
            "datetime64[ns]"
        )

    @staticmethod
    def date(series):

        return (
            Normalizer._safe_datetime(series)
            .dt.normalize()
        )

    @staticmethod
    def datetime(series):

        return Normalizer._safe_datetime(
            series
        )

    @staticmethod
    def number(series):

        return (
            pd.to_numeric(
                series,
                errors="coerce"
            )
            .fillna(0)
        )

    @staticmethod
    def normalize_asn(df):

        df = df.copy()

        df["BN"] = Normalizer.text(
            df["Batch Number"]
        )

        df["Expiry Date"] = Normalizer.date(
            df["Expiration Date"]
        )

        df["Received Quantity"] = Normalizer.number(
            df["Received Qty"]
        )

        df["Trade Name"] = Normalizer.text(
            df["Trade Description"]
        )

        df["Trade Item"] = Normalizer.identifier(
            df["Trade Item"]
        )

        df["Inbound Shipment"] = Normalizer.identifier(
            df["Inbound Shipment"]
        )

        df["ASN Line"] = Normalizer.identifier(
            df["ASN Line"]
        )

        df["Supplier Name"] = Normalizer.text(
            df["Supplier Name"]
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
                    "Transaction Date"
                ]
            )
        )

        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Generic Item Number",
                    "Item Number",
                    "Generic Number"
                ]
            )
        )

        df["Supplier Code"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Supplier Code",
                    "Vendor Code"
                ]
            )
        )

        df["PO Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "PO Number",
                    "Purchase Order Number",
                    "NUPCO PO"
                ]
            )
        )

        df["Invoice Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Invoice Number",
                    "Invoice"
                ]
            )
        )

        return df

    @staticmethod
    def normalize_inventory(df):

        df = df.copy()

        df["BN"] = Normalizer.text(
            df["Lot No/Batch"]
        )

        df["Expiry Date"] = Normalizer.date(
            df["Expiry Date"]
        )

        df["Available Quantity"] = Normalizer.number(
            df["Available Qty"]
        )

        df["Trade Name"] = Normalizer.text(
            df["Trade Item Description"]
        )

        df["Inventory Snapshot Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Snapshot Date",
                    "Report Date",
                    "Inventory Date",
                    "Date Created",
                    "Created Date",
                    "As Of Date"
                ]
            )
        )

        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Generic Item Number",
                    "Item Number"
                ]
            )
        )

        df["Trade Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Trade Item Number",
                    "Trade Item"
                ]
            )
        )

        return df

    @staticmethod
    def normalize_dispatch(df):

        df = df.copy()

        df["BN"] = Normalizer.text(
            df["Batch/Lot"]
        )

        df["Expiry Date"] = Normalizer.date(
            df["Best Before Date"]
        )

        df["Dispatched Quantity"] = Normalizer.number(
            df["Pick Qty"]
        )

        df["Trade Name"] = Normalizer.text(
            df["Trade Description"]
        )

        df["Trade Item Number"] = Normalizer.identifier(
            df["Trade Item Number"]
        )

        df["To Address"] = Normalizer.text(
            df["To Address"]
        )

        df["Sales Order Number"] = Normalizer.identifier(
            df["Sales Order Number"]
        )

        df["Order Line"] = Normalizer.identifier(
            df["order line"]
        )

        df["Dispatch Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Dispatched Date",
                    "Dispatch Date",
                    "Actual Dispatch Date",
                    "Ship Date",
                    "Shipment Date",
                    "Date Dispatched",
                    "Transaction Date"
                ]
            )
        )

        df["Generic Item Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Generic Item Number",
                    "Item Number"
                ]
            )
        )

        df["Reference Order Number"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Reference order #",
                    "Reference Order Number",
                    "Reference Order"
                ]
            )
        )

        df["Ship To Customer"] = Normalizer.identifier(
            Normalizer._optional_series(
                df,
                [
                    "Ship To Customer",
                    "Customer Code"
                ]
            )
        )

        return df

    @staticmethod
    def normalize_sfda(df):

        df = df.copy()

        df["GTIN"] = Normalizer.identifier(
            df["GTIN"],
            length=14
        )

        df["BN"] = Normalizer.text(
            df["BN"]
        )

        df["Expiry Date"] = Normalizer.date(
            df["Expiry Date"]
        )

        df["Drug Name"] = Normalizer.text(
            df["Drug Name"]
        )

        df["Quantity"] = Normalizer.number(
            df["Quantity"]
        )

        df["Active"] = Normalizer.number(
            df["Active"]
        )

        df["Quantity Receive Pending"] = Normalizer.number(
            df["Quantity Receive Pending"]
        )

        df["Quantity sent pending"] = Normalizer.number(
            df["Quantity sent pending"]
        )

        df["SFDA Snapshot Date"] = Normalizer.datetime(
            Normalizer._optional_series(
                df,
                [
                    "Snapshot Date",
                    "Report Date",
                    "Drug Count Date",
                    "Date Created",
                    "Created Date",
                    "As Of Date"
                ]
            )
        )

        return df

    @staticmethod
    def normalize_packsize(df):

        df = df.copy()

        df["Trade Name"] = Normalizer.text(
            df["Trade Name"]
        )

        df["PackageSize"] = Normalizer.number(
            df["PackageSize"]
        )

        return df

    @staticmethod
    def normalize_gln(df):

        df = df.copy()

        df["To Address"] = Normalizer.text(
            df["To Address"]
        )

        df["GLN"] = Normalizer.identifier(
            df["GLN"]
        )

        df = df[
            (df["To Address"] != "")
            & (df["GLN"] != "")
        ].copy()

        df = df.drop_duplicates(
            subset=["To Address"],
            keep="first"
        )

        return df.reset_index(
            drop=True
        )
