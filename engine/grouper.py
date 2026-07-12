class Grouper:

    @staticmethod
    def summarize(df, keys, quantity_column):

        grouped = (
            df.groupby(
                keys,
                dropna=False,
                as_index=False
            )[quantity_column]
            .sum()
        )

        return grouped
