import pandas as pd


class Grouper:

    @staticmethod
    def summarize(df, keys, quantity_column):
        if df is None or df.empty:
            return pd.DataFrame(columns=[*keys, quantity_column])

        source = df.copy()
        source[quantity_column] = pd.to_numeric(
            source[quantity_column],
            errors="coerce",
        ).fillna(0)

        grouped = (
            source.groupby(
                keys,
                dropna=False,
                as_index=False,
            )[quantity_column]
            .sum()
        )

        return grouped
