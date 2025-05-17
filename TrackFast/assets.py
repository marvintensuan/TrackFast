from pathlib import Path

from dagster import asset, MaterializeResult, MetadataValue

import polars as pl

@asset
def transactions() -> MaterializeResult:
    """Return a DataFrame of transactions from `io/gemini`."""

    gemini = Path("io/gemini")

    txns = []

    for file in gemini.iterdir():
        if file.suffix == ".json":
            df = pl.read_json(file)
            txns.append(df)

    transactions: pl.DataFrame = pl.concat(txns)

    return MaterializeResult(
        metadata={"file_path": str(gemini), "preview": MetadataValue.md(str(transactions.head(5)))}
    )