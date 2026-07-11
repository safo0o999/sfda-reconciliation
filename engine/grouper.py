class Grouper:

    @staticmethod
    def summarize(df, keys, quantity_column):

        return (
            df.groupby(
                keys,
                as_index=False
            )[quantity_column]
            .sum()
        )
