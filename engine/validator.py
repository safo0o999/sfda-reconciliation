from config.schema import REQUIRED_COLUMNS


class Validator:
    @staticmethod
    def validate(df, report_name):
        if report_name not in REQUIRED_COLUMNS:
            raise ValueError(
                f"Unknown report type: {report_name}"
            )

        if df is None:
            raise ValueError(
                f"{report_name} dataframe is missing."
            )

        required = REQUIRED_COLUMNS[report_name]

        missing = [
            column
            for column in required
            if column not in df.columns
        ]

        if missing:
            raise ValueError(
                f"{report_name} missing columns: {missing}"
            )

        return True
