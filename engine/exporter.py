import io
import pandas as pd


class Exporter:

    @staticmethod
    def to_csv(df):

        output = io.StringIO()

        df.to_csv(
            output,
            index=False
        )

        return output.getvalue().encode("utf-8")
