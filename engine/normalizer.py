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
