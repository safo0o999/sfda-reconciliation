class Grouper:

    @staticmethod
    def summarize(df, keys, quantity_column):
        if df is None or df.empty:
            return df.reindex(columns=[*keys, quantity_column]).copy()
        return (
            df.groupby(keys, dropna=False, as_index=False)[quantity_column]
            .sum()
        )
