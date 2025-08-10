import hashlib
from pathlib import Path

from dagster import asset #, MaterializeResult, MetadataValue

import polars as pl


def sha256_concat(row) -> str:
    concat = f"{row['Provider']}{row['Date']}{row['Transaction']}{row['Amount']}"
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


@asset
def transactions(context):
    """Return a DataFrame of transactions from `io/gemini`."""

    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_tbl_hide_column_data_types(True)
    
    gemini = Path("io/gemini")

    txns = []

    for file in gemini.iterdir():
        if file.suffix == ".json":
            context.log.info(f"Reading {file.name}...")
            df = pl.read_json(file)
            context.log.info(f"{df.head()}")
            txns.append(df)

    transactions: pl.DataFrame = pl.concat(txns)

    transactions_with_id = transactions.with_columns(
        pl.struct(["Provider", "Date", "Transaction", "Amount"])
        .map_elements(sha256_concat)
        .alias("id")
    )


    
    # return MaterializeResult(
    #     metadata={"file_path": str(gemini), "preview": MetadataValue.md(str(transactions_with_id.head(5)))}
    # )

    return transactions_with_id