from collections.abc import Mapping
from pathlib import Path

import polars as pl
import polars.selectors as cs
import pyreadr  # type: ignore

from cdef_cohort_generation.config import ICD_FILE, ISCED_FILE, RDAT_FILE
from cdef_cohort_generation.logging_config import log


def parse_dates(col_name: str) -> pl.Expr:
    return pl.coalesce(
        # Prioritize formats with '/' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d %H:%M:%S", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%y", strict=False),
        # LPR3 format for dates
        pl.col(col_name).str.strptime(pl.Date, "%d%b%Y", strict=False),
        # Then formats with '-' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d-%m-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m-%d-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False),
        # Locale's appropriate date and time representation
        pl.col(col_name).str.strptime(pl.Date, "%c", strict=False),
    )


def read_isced_data() -> pl.LazyFrame:
    """Read and process ISCED data from Rdata file."""
    try:
        if ISCED_FILE.exists():
            log("Reading ISCED data from existing parquet file...")
            return pl.scan_parquet(ISCED_FILE)
        else:
            log("Processing ISCED data from Rdata file...")
            result = pyreadr.read_r(RDAT_FILE, use_objects=["uddf"])
            isced_data = pl.from_dict(result)
            isced_data = isced_data.select(
                pl.col("uddf").struct.field("HFAUDD").cast(pl.Utf8),
                pl.col("uddf").struct.field("HFAUDD_isced"),
            )
            isced_final = (
                isced_data.with_columns(
                    [
                        pl.col("HFAUDD").str.strip_suffix(".0"),
                        pl.col("HFAUDD_isced").str.slice(0, 1).alias("EDU_LVL"),
                    ]
                )
                .unique()
                .select(["HFAUDD", "EDU_LVL"])
            )
            isced_final.write_parquet(ISCED_FILE)
            return isced_final.lazy()
    except Exception as e:
        log(f"Error processing ISCED data: {e}")
        raise


def process_register_data(
    input_files: Path,
    output_file: Path,
    population_file: Path,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    date_columns: list[str] | None = None,
    columns_to_keep: list[str] | None = None,
    join_on: str | list[str] = "PNR",
    join_parents_only: bool = False,
    register_name: str = "",
) -> None:
    """
    Process register data, join with population data, and save the result.

    Args:
    input_files (Path): Path to input parquet files.
    output_file (Path): Path to save the output parquet file.
    population_file (Path): Path to the population parquet file.
    schema (Dict[str, pl.DataType]): Schema for the input data.
    date_columns (Optional[List[str]]): List of column names to parse as dates.
    columns_to_keep (Optional[List[str]]): List of columns to keep in the final output.
    join_on (str | List[str]): Column(s) to join on. Default is "PNR".
    join_parents_only (bool): If True, only join on FAR_ID and MOR_ID. Default is False.
    register_name (str): Name of the register being processed. Default is "".

    Returns:
    None
    """
    # Read all input parquet files
    data = pl.scan_parquet(input_files, allow_missing_columns=True)

    # Parse date columns if specified
    if date_columns:
        for col in date_columns:
            data = data.with_columns(parse_dates(col).alias(col))

    # Select specific columns if specified
    if columns_to_keep:
        data = data.select(columns_to_keep)

    # Special handling for UDDF register
    if register_name.lower() == "uddf":
        isced_data = read_isced_data()
        data = data.join(isced_data, left_on="HFAUDD", right_on="HFAUDD", how="left")

    # Read in the population file
    population = pl.scan_parquet(population_file)

    # Prepare result dataframe
    result = population

    # If joining on parents, we need to join twice more for parent-specific data
    if join_parents_only:
        result = result.join(
            data.select(cs.starts_with("FAR_")),
            left_on="FAR_ID",
            right_on=f"FAR_{join_on}",
            how="left",
        )
        result = result.join(
            data.select(cs.starts_with("MOR_")),
            left_on="MOR_ID",
            right_on=f"MOR_{join_on}",
            how="left",
        )
    else:
        # Join on specified column(s)
        join_columns = [join_on] if isinstance(join_on, str) else join_on
        result = result.join(data, on=join_columns, how="left")

    # Collect and save the result
    result.collect().write_parquet(output_file)


def read_icd_descriptions() -> pl.LazyFrame:
    """Read ICD-10 code descriptions."""
    return pl.scan_csv(ICD_FILE)


def apply_scd_algorithm(df: pl.LazyFrame) -> pl.LazyFrame:
    """Apply the SCD (Severe Chronic Disease) algorithm."""
    icd_prefixes = [
        "C",
        "D61",
        "D76",
        "D8",
        "E10",
        "E25",
        "E7",
        "G12",
        "G31",
        "G37",
        "G40",
        "G60",
        "G70",
        "G71",
        "G73",
        "G80",
        "G81",
        "G82",
        "G91",
        "G94",
        "I12",
        "I27",
        "I3",
        "I4",
        "I5",
        "J44",
        "J84",
        "K21",
        "K5",
        "K7",
        "K90",
        "M3",
        "N0",
        "N13",
        "N18",
        "N19",
        "N25",
        "N26",
        "N27",
        "P27",
        "P57",
        "P91",
        "Q0",
        "Q2",
        "Q3",
        "Q4",
        "Q6",
        "Q79",
        "Q86",
        "Q87",
        "Q9",
    ]
    specific_codes = [
        "D610",
        "D613",
        "D618",
        "D619",
        "D762",
        "E730",
        "G310",
        "G318",
        "G319",
        "G702",
        "G710",
        "G711",
        "G712",
        "G713",
        "G736",
        "G811",
        "G821",
        "G824",
        "G941",
        "J448",
        "P910",
        "P911",
        "P912",
        "Q790",
        "Q792",
        "Q793",
        "Q860",
    ]

    df_with_scd = df.with_columns(
        is_scd=(
            pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 4).is_in(icd_prefixes)
            | (
                (pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 4) >= "E74")
                & (pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 4) <= "E84")
            )
            | pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 5).is_in(specific_codes)
            | (
                (pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 5) >= "P941")
                & (pl.col("C_ADIAG").str.to_uppercase().str.slice(1, 5) <= "P949")
            )
            | pl.col("C_DIAG").str.to_uppercase().str.slice(1, 4).is_in(icd_prefixes)
            | (
                (pl.col("C_DIAG").str.to_uppercase().str.slice(1, 4) >= "E74")
                & (pl.col("C_DIAG").str.to_uppercase().str.slice(1, 4) <= "E84")
            )
            | pl.col("C_DIAG").str.to_uppercase().str.slice(1, 5).is_in(specific_codes)
            | (
                (pl.col("C_DIAG").str.to_uppercase().str.slice(1, 5) >= "P941")
                & (pl.col("C_DIAG").str.to_uppercase().str.slice(1, 5) <= "P949")
            )
        )
    )

    # Add first SCD diagnosis date
    df_with_scd = df_with_scd.with_columns(
        first_scd_date=pl.when(pl.col("is_scd"))
        .then(pl.col("D_INDDTO"))
        .otherwise(None)
        .first()
        .over("PNR")
    )

    return df_with_scd


def add_icd_descriptions(df: pl.LazyFrame, icd_descriptions: pl.LazyFrame) -> pl.LazyFrame:
    """Add ICD-10 descriptions to the dataframe."""
    return (
        df.with_columns(
            [
                pl.col("C_ADIAG").str.slice(1).alias("icd_code_adiag"),
                pl.col("C_DIAG").str.slice(1).alias("icd_code_diag"),
            ]
        )
        .join(
            icd_descriptions,
            left_on="icd_code_adiag",
            right_on="icd10",
            how="left",
        )
        .join(
            icd_descriptions,
            left_on="icd_code_diag",
            right_on="icd10",
            how="left",
            suffix="_diag",
        )
        .drop(["icd_code_adiag", "icd_code_diag"])
    )
