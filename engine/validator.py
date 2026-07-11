from config.schema import REQUIRED_COLUMNS


class Validator:

    @staticmethod
    def validate(df, report_name):

        required = REQUIRED_COLUMNS[report_name]

        missing = [
            col
            for col in required
            if col not in df.columns
        ]

        if missing:
            raise ValueError(
                f"{report_name} missing columns: {missing}"
            )

        return True
