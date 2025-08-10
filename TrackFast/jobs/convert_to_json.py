import json
from pathlib import Path
from typing import Iterable

from dagster import job, op, In, Out
from google.genai import Client

from my_gemini_requestor import GeminiRequestor

from TrackFast.jobs.utils import (
    get_input_files,
    read_file_safely,
    write_all_processed_files,
)


@op
def gemini_api_key() -> str:
    """Retrieve API key from `creds/GEMINI_API_KEY`."""
    credentials = Path("creds/GEMINI_API_KEY")

    if not credentials.exists():
        raise FileNotFoundError("`creds/GEMINI_API_KEY` was not found.")

    return credentials.read_text()


@op(out={"result": Out(dagster_type=set)})
def get_unprocessed_readable_files() -> set[Path]:
    """Return a list of files from `inputs_readable` that haven't been processed yet."""
    readable_files = get_input_files("io/inputs_readable")

    contents = read_file_safely("io/outputs/files_processed_by_gemini.json")
    processed_files: list[str] = json.loads(contents)
    existing_files: set[Path] = {Path(file) for file in processed_files}

    return readable_files - existing_files


@op(ins={"file_paths": In(dagster_type=set)})
def call_gemini_api(file_paths: Iterable, gemini_api_key: str) -> dict[Path, str]:
    """Sends files to the Gemini API for processing and retrieves responses."""

    client = Client(api_key=gemini_api_key, model="gemini-2.5-flash")

    responses = {}
    
    prompts = {
        "BPI": Path("io/prompts/bpi_estatement.txt").read_text(),
        "UB": Path("io/prompts/ub_statement.txt").read_text(),
    }

    def get_prompt_key(file: Path) -> str:
        """Get the file key for prompt lookup."""
        filename = file.name

        if "BPI" in filename:
            return "BPI"
        if "UB REWARDS" in filename:
            return "UB"
        
        raise ValueError(f"File {filename} does not match any known keys.")


    for file in file_paths:
        key = get_prompt_key(file)
        prompt = prompts[key]
        requestor = GeminiRequestor(prompt=prompt, file_path=file, client=client)

        requestor.send_request()

        responses[file] = requestor.response

    return responses


@op
def write_json_files(results: dict) -> None:
    """Write JSON responses."""
    parent = Path("io/gemini")
    for path, response in results.items():
        stem = path.stem
        output = parent / f"{stem}.json"
        output.write_text(response, encoding="utf-8")


@op
def store_gemini_results(results: dict) -> None:
    """Write a JSON file to recognize files already processed."""

    path = Path("./io/outputs/files_processed_by_gemini.json")
    old_data = set()

    if path.exists():
        contents = path.read_text()
        if contents:
            old_data = set(json.loads(contents))

    stringify = {str(file.stem) for file in results.keys()}

    write_all_processed_files(
        file_name=path,
        old_data=old_data,
        new_data=stringify,
    )


@job
def generate_json_files_from_raw_files() -> None:
    """Generate JSON files from raw files using the Gemini API."""

    api_key = gemini_api_key()
    unprocessed_files = get_unprocessed_readable_files()
    results = call_gemini_api(file_paths=unprocessed_files, gemini_api_key=api_key)
    write_json_files(results)
    store_gemini_results(results)
