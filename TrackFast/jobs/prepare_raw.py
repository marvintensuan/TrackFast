"""
Get all files in `io/inputs_raw` and use
`qpdf` to remove password and save to `io/inputs_readable`.
"""

import json
from pathlib import Path
import shutil
import subprocess

from dagster import job, op


@op
def check_qpdf_installed() -> str | None:
    """Checks if `qpdf` is installed and exists in `PATH`."""
    qpdf = shutil.which("qpdf")

    if qpdf is None:
        raise FileNotFoundError("`qpdf` is not found.")

    return qpdf


@op
def get_unprocessed_raw_files() -> list[Path]:
    """Return a list of files from `inputs_raw` that haven't been processed yet."""
    input_dir = Path("./io/inputs_raw")
    processed_log_path = Path("./io/outputs/raw_files_results.json")

    input_files = list(input_dir.iterdir())
    input_files_set = set(input_files)

    if not processed_log_path.exists() or processed_log_path.read_text().strip() == "":
        return input_files

    processed_files_list = json.loads(processed_log_path.read_text())
    processed_files_set = {Path(file) for file in processed_files_list}

    unprocessed_files = input_files_set - processed_files_set

    return list(unprocessed_files)


@op
def process_raw_files(qpdf: str | None, files: list[Path] | None) -> set[str]:
    pw = Path("./creds/BPI_STATEMENT").read_text()

    status: set[str] = set()
    for file in files:
        filename = str(file)
        result = subprocess.run(
            [
                qpdf,
                f"--password={pw}",
                "--decrypt",
                filename,
                f"./io/inputs_readable/{file.name}",
            ]
        )

        if not result.returncode:
            status.add(filename)

    return status


@op
def store_results(results: set[str]) -> None:
    """Write a JSON file to preserve files already converted."""

    if not results:
        return

    path = Path("./io/outputs/raw_files_results.json")
    old_data = set()

    if path.exists():
        contents = path.read_text()
        if contents:
            old_data = set(json.loads(contents))

    data = results | old_data

    path.write_text(json.dumps(sorted(data), indent=2))


@job
def prepare_input_by_password_removal() -> None:
    """Remove passwords from password-protected PDFs."""
    qpdf = check_qpdf_installed()
    raw_files = get_unprocessed_raw_files()
    results = process_raw_files(qpdf, raw_files)
    store_results(results)
