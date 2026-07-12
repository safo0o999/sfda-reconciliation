import pandas as pd


class Normalizer:

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
    def date(series):

        return (
            pd.to_datetime(
                series,
                errors="coerce",
                dayfirst=True
            )
            .dt.normalize()
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

        df["To Address"] = Normalizer.text(
            df["To Address"]
        )

        df["Sales Order Number"] = Normalizer.identifier(
            df["Sales Order Number"]
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
