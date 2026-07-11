from pathlib import Path


class Validator:

    REQUIRED_FILES = [
        "asn",
        "inventory",
        "dispatch",
        "sfda",
        "packsize"
    ]

    @staticmethod
    def validate_files(files):

        for file in Validator.REQUIRED_FILES:

            if file not in files:
                raise Exception(f"Missing file: {file}")

            if files[file] is None:
                raise Exception(f"Empty file: {file}")

    @staticmethod
    def validate_dataframe(df, required_columns):

        missing = []

        for column in required_columns:

            if column not in df.columns:
                missing.append(column)

        if missing:

            raise Exception(
                "Missing columns: " + ", ".join(missing)
            )
