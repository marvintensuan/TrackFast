"""
Get all files in `io/inputs_raw` and use
`qpdf` to remove password and save to `io/inputs_readable`.
"""

import json
from pathlib import Path
import shutil
import subprocess
from typing import Iterable

from dagster import job, op, In, Out

from TrackFast.jobs.utils import (
    get_input_files,
    read_file_safely,
    write_all_processed_files,
)


@op
def check_qpdf_installed() -> str:
    """Checks if `qpdf` is installed and exists in `PATH`."""
    qpdf = shutil.which("qpdf")

    if qpdf is None:
        raise FileNotFoundError("`qpdf` is not found.")

    return qpdf


@op
def get_unprocessed_raw_files() -> set[Path]:
    """Return a list of files from `inputs_raw` that haven't been processed yet."""

    files_from_inputs = get_input_files("io/inputs_raw")

    contents = read_file_safely("io/outputs/raw_files_results.json")
    processed_files: list[str] = json.loads(contents)
    existing_files: set[Path] = {Path(file) for file in processed_files}

    return files_from_inputs - existing_files


@op(ins={"files": In(dagster_type=set)}, out={"result": Out(dagster_type=set)})
def process_raw_files(qpdf: str, files: Iterable[Path] | None) -> set | set[str]:
    """Use `qpdf` to generate password-less files."""

    pw = Path("./creds/BPI_STATEMENT").read_text()

    status: set[str] = set()

    if files is None:
        return status

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

    path = Path("io/outputs/raw_files_results.json")
    old_data = set()

    if path.exists():
        contents = path.read_text()
        if contents:
            old_data = set(json.loads(contents))

    write_all_processed_files(
        file_name=path,
        old_data=old_data,
        new_data=results,
    )


@job
def prepare_input_by_password_removal() -> None:
    """Remove passwords from password-protected PDFs."""
    qpdf = check_qpdf_installed()
    raw_files = get_unprocessed_raw_files()
    results = process_raw_files(qpdf, raw_files)
    store_results(results)
