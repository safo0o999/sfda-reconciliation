from pathlib import Path
import pandas as pd


class ExcelReader:

    def __init__(self):
        pass

    @staticmethod
    def get_engine(file_path: Path):

        if file_path.suffix.lower() == ".xls":
            return "xlrd"

        return "openpyxl"

    @staticmethod
    def read(file_path):

        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(file_path)

        return pd.read_excel(
            file_path,
            engine=ExcelReader.get_engine(file_path),
            dtype=object
        )
