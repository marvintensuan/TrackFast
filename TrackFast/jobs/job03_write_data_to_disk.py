import hashlib
from pathlib import Path

from dagster import op, job, In

import polars as pl


def _sha256_concat(row) -> str:
    concat = f"{row['Provider']}{row['Date']}{row['Transaction']}{row['Amount']}"
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


@op
def get_json_files(file_path: str = "io/gemini") -> list[Path]:
    """Get all JSON files from `file_path`."""
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
    """Assign unique IDs to each transaction based on its content,
    placing the `id` column first."""

    transactions_with_id = transactions.with_columns(
        pl.struct(["Provider", "Date", "Transaction", "Amount"])
        .map_elements(_sha256_concat)
        .alias("id")
    )

    # Reorder columns so that `id` comes first
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
def generate_files_from_json():
    """Job to compile JSON transaction files into a single Parquet file with unique IDs."""
    json_files = get_json_files()
    concatenated_transactions = concatenate_json_files(json_files)
    transactions_with_ids = assign_unique_ids(concatenated_transactions)
    write_transaction_parquet(transactions_with_ids)
    write_transaction_csv(transactions_with_ids)
