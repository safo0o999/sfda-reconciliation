import pandas as pd


class Normalizer:

    @staticmethod
    def clean_text(series):

        return (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )

    @staticmethod
    def clean_date(series):

        return pd.to_datetime(
            series,
            errors="coerce",
            dayfirst=True
        ).dt.normalize()

    @staticmethod
    def clean_number(series):

        return pd.to_numeric(
            series,
            errors="coerce"
        ).fillna(0)

    @staticmethod
    def create_key(df, trade_item, batch, expiry):

        df["KEY"] = (
            df[trade_item].astype(str)
            + "|"
            + df[batch].astype(str)
            + "|"
            + df[expiry].astype(str)
        )

        return df
