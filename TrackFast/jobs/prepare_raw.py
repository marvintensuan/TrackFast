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
def get_raw_files() -> list[Path]:
    """Return a list of files  from `inputs_raw`."""
    raw_files_all = [*Path("./io/inputs_raw").iterdir()]
    all_files = set(raw_files_all)

    with open("io/outputs/raw_files_results.json") as file:
        contents = file.read()
    
    if contents == "":
        return raw_files_all

    already_read: dict[str, bool] = json.loads(contents)
    already_read_path = {Path(file) for file in already_read}

    for_processing = all_files - already_read_path

    return [*for_processing]


@op
def process_raw_files(qpdf: str | None, files: list[Path] | None) -> set[str]:
    pw = Path("./creds/BPI_STATEMENT").read_text()

    status = set()
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
def store_results(results: set) -> None:
    """Write a JSON file to preserve files already converted."""
    with open("./io/outputs/raw_files_results.json", "w+") as file:
        contents = file.read()
        if contents == "":
            data = [*results]
        else:
            old_data = set(json.loads(contents))
            data = results | old_data
            
        
        data = json.dumps([*data], indent=2)

        file.write(data)


@job
def prepare_input_by_password_removal() -> None:
    """Remove passwords from password-protected PDFs."""
    qpdf = check_qpdf_installed()
    raw_files = get_raw_files()
    results = process_raw_files(qpdf, raw_files)
    store_results(results)
