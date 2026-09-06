import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any, Iterable

from dagster import job, op, In, Out
from google.genai import Client
import polars as pl

from my_gemini_requestor import GeminiRequestor

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
    """Return a set of files from `inputs_raw` that haven't been processed yet.
    
    Read a list of files processed from `io/outputs/raw_files_results.json` and compare it to the files in `io/inputs_raw`.
    """

    files_from_inputs = get_input_files("io/inputs_raw")

    contents = read_file_safely("io/outputs/raw_files_results.json")
    processed_files: list[str] = json.loads(contents)
    existing_files: set[Path] = {Path(file) for file in processed_files}

    return files_from_inputs - existing_files


@op(ins={"files": In(dagster_type=set)}, out={"result": Out(dagster_type=set)})
def process_raw_files(context, qpdf: str, files: Iterable[Path] | None) -> set | set[str]:
    """Use `qpdf` to generate password-less files.
    
    Write the password-less files to `io/inputs_readable` and return a set of successfully processed files.
    Returns a set of successfully processed files. If no files are provided, returns an empty set.
    """

    def convert(file: Path, password: str) -> Any:
        """Convert a file using `qpdf`."""
        return subprocess.run(
            [
                qpdf,
                f"--password={password}",
                "--decrypt",
                str(file),
                f"./io/inputs_readable/{file.name}",
            ]
        )

    def get_file_key(file: Path) -> str:
        """Get the file key for password lookup."""
        filename = file.name

        if "BPI" in filename:
            return "BPI"
        if "UB REWARDS" in filename:
            return "UB"
        if "UB_MC201" in filename:
            return "UB2"
        
        raise ValueError(f"File {filename} does not match any known keys.")

    passwords = {
        "UB": Path("./creds/UB_STATEMENT").read_text(),
        "UB2": Path("./creds/UB_STATEMENT2").read_text(),
        "BPI": Path("./creds/BPI_STATEMENT").read_text(),
    }


    status: set[str] = set()

    if files is None:
        return status

    for file in files:
        key = get_file_key(file)
        pw = passwords[key]
        result: subprocess.CompletedProcess = convert(file, pw)
        context.log.info(f"{result.returncode=} for file {file.name}")

        if not result.returncode:
            status.add(str(file))

    return status


@op
def store_results(results: set[str]) -> None:
    """Write a JSON file to store file names already converted."""

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


@op
def gemini_api_key() -> str:
    """Retrieve API key from `creds/GEMINI_API_KEY`."""
    credentials = Path("creds/GEMINI_API_KEY")

    if not credentials.exists():
        raise FileNotFoundError("`creds/GEMINI_API_KEY` was not found.")

    return credentials.read_text()


@op(out={"result": Out(dagster_type=set)})
def get_unprocessed_readable_files(context) -> set[Path]:
    """Return a list of files from `inputs_readable` that haven't been processed yet."""
    readable_files = get_input_files("io/inputs_readable")

    contents = read_file_safely("io/outputs/files_processed_by_gemini.json")
    processed_files: list[str] = json.loads(contents)
    existing_files: set[Path] = {
        Path("io/inputs_readable") / file for file in processed_files
    }

    context.log.info(f"All files: {readable_files=}")
    context.log.info(f"Existing files: {existing_files=}")

    files_to_process = readable_files - existing_files

    if files_to_process:
        context.log.info(f"Files to process: {files_to_process=}")
    else:
        context.log.info("No files to process.")

    return files_to_process


@op(ins={"file_paths": In(dagster_type=set)})
def call_gemini_api(
    context, file_paths: Iterable, gemini_api_key: str
) -> dict[Path, str]:
    """Sends files to the Gemini API for processing and retrieves responses."""

    if not file_paths:
        context.log.info("No files to process.")
        return {}

    client = Client(api_key=gemini_api_key)

    responses = {}

    prompts = {
        "BPI": Path("io/prompts/bpi_estatement.txt").read_text(),
        "UB": Path("io/prompts/ub_statement.txt").read_text(),
    }

    def get_prompt_key(file: Path) -> str:
        """Get the file key for prompt lookup."""
        filename = file.name
        context.log.info(f"Determining prompt for file: {filename}")
        if "BPI" in filename:
            return "BPI"
        if "UB REWARDS" in filename:
            return "UB"
        if "UB_MC201" in filename:
            return "UB"

        raise ValueError(f"File {filename} does not match any known keys.")

    for file in file_paths:
        context.log.info(f"Sending file to Gemini: {file.name}...")
        key = get_prompt_key(file)
        prompt = prompts[key]
        requestor = GeminiRequestor(
            prompt=prompt, file_path=file, client=client, model="gemini-3.5-flash"
        )

        requestor.send_request()

        responses[file] = requestor.response

    return responses


@op
def write_json_files(results: dict) -> bool:
    """Write JSON responses."""
    parent = Path("io/gemini")
    for path, response in results.items():
        stem = path.stem
        output = parent / f"{stem}.json"
        output.write_text(response, encoding="utf-8")

    return True


@op
def store_gemini_results(results: dict) -> None:
    """Write a JSON file to recognize files already processed."""

    path = Path("./io/outputs/files_processed_by_gemini.json")
    old_data = set()

    if path.exists():
        contents = path.read_text()
        if contents:
            old_data = set(json.loads(contents))

    stringify = {str(file.name) for file in results.keys()}

    write_all_processed_files(
        file_name=path,
        old_data=old_data,
        new_data=stringify,
    )

def _sha256_concat(row) -> str:
    concat = (
        f"{row['index']}"
        f"{row['Provider']}"
        f"{row['Date']}"
        f"{row['Transaction']}"
        f"{row['Amount']}"
    )
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


@op
def get_json_files(is_complete, file_path: str = "io/gemini") -> list[Path]:
    """Get all JSON files from `file_path`."""
    if not is_complete:
        raise ValueError("Please check previous step. JSON files are not complete.")
    parent = Path(file_path)
    file_paths = [file for file in parent.iterdir() if file.suffix == ".json"]
    return file_paths


@op(ins={"files": In(dagster_type=list)})
def concatenate_json_files(context, files: list[Path]) -> pl.DataFrame:
    """Concatenate JSON files into a single DataFrame."""
    dataframes = []

    for file in files:
        context.log.info(f"Reading {file.name}...")
        df = pl.read_json(file)
        dataframes.append(df)

    concatenated: pl.DataFrame = pl.concat(dataframes)

    return concatenated


@op
def assign_unique_ids(transactions: pl.DataFrame) -> pl.DataFrame:
    """Assign unique IDs to each transaction based on its content + row index,
    placing the `id` column first."""

    transactions = transactions.sort(["Date", "Transaction", "Amount"])

    transactions_with_id = (
        transactions.with_row_count("index")
        .with_columns(
            pl.struct(["index", "Provider", "Date", "Transaction", "Amount"])
            .map_elements(_sha256_concat)
            .alias("id")
        )
        .drop("index")
    )

    cols = ["id"] + [c for c in transactions_with_id.columns if c != "id"]
    return transactions_with_id.select(cols)


@op
def write_transaction_parquet(
    context,
    transactions: pl.DataFrame,
    output_path: str = "io/outputs/transactions.parquet",
) -> None:
    """Write transactions DataFrame to Parquet file."""
    output = Path(output_path)
    transactions.write_parquet(output)
    context.log.info(f"Done writing {output_path}.")


@op
def write_transaction_csv(
    context,
    transactions: pl.DataFrame,
    output_path: str = "io/outputs/transactions.csv",
) -> None:
    """Write transactions DataFrame to CSV file."""
    output = Path(output_path)
    transactions.write_csv(output, quote_style="always")
    context.log.info(f"Done writing {output_path}.")

@job
def job01_create_files_from_raw_end2end() -> None:
    """Main job to process raw files, call Gemini API, and generate transactions."""
    qpdf = check_qpdf_installed()
    unprocessed_raw_files = get_unprocessed_raw_files()
    processed_raw_files = process_raw_files(qpdf=qpdf, files=unprocessed_raw_files)
    store_results(results=processed_raw_files)

    gemini_key = gemini_api_key()

    gemini_results = call_gemini_api(
        file_paths=processed_raw_files, gemini_api_key=gemini_key
    )
    is_json_complete = write_json_files(results=gemini_results)
    store_gemini_results(results=gemini_results)

    json_files = get_json_files(is_complete=is_json_complete)
    concatenated_transactions = concatenate_json_files(files=json_files)
    transactions_with_ids = assign_unique_ids(transactions=concatenated_transactions)
    write_transaction_parquet(transactions=transactions_with_ids)
    write_transaction_csv(transactions=transactions_with_ids)