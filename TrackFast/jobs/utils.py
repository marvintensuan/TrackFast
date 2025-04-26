from collections.abc import Set, KeysView
import json
from pathlib import Path

type SetLike = Set | KeysView


def get_input_files(file_path: str | Path) -> set[Path]:
    """Return a set of `pathlib.Path` from `file_path`."""
    input_dir = Path(file_path)
    input_files = [*input_dir.iterdir()]
    return set(input_files)


def read_file_safely(file_name: str | Path) -> str:
    """Return file contents that is safe for `json.loads` consumption."""
    file = Path(file_name)
    contents = file.read_text().strip()
    if not contents:
        return "[]"
    return contents


def write_all_processed_files(
    file_name: str | Path, old_data: SetLike, new_data: SetLike
) -> None:
    """Write a JSON file to record files already processed."""

    file = Path(file_name)

    data = new_data | old_data
    sorted_data = sorted(data)
    convert_to_str = json.dumps(sorted_data, indent=2)

    file.write_text(convert_to_str, encoding="utf-8")
