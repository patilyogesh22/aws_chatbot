import csv
import os
import tempfile
from pathlib import Path

import openpyxl


def convert_excel_to_csv(file_obj, original_filename):
    """
    Convert uploaded Excel file to CSV.
    Returns:
        csv_path
        csv_filename
    """

    suffix = Path(original_filename).suffix.lower()

    if suffix not in [".xlsx", ".xls"]:
        return None, original_filename

    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=suffix
    ) as temp_excel:

        file_obj.seek(0)
        temp_excel.write(file_obj.read())

        excel_path = temp_excel.name

    workbook = openpyxl.load_workbook(
        excel_path,
        data_only=True
    )

    sheet = workbook.active

    csv_filename = (
        Path(original_filename).stem + ".csv"
    )

    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".csv",
        mode="w",
        newline="",
        encoding="utf-8"
    ) as temp_csv:

        writer = csv.writer(temp_csv)

        for row in sheet.iter_rows(values_only=True):
            writer.writerow(row)

        csv_path = temp_csv.name

    os.remove(excel_path)

    return csv_path, csv_filename