import polars as pl

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.utils import ICD_FILE


def read_icd_descriptions() -> pl.LazyFrame:
    """Read ICD-10 code descriptions."""
    return pl.scan_csv(ICD_FILE)


def apply_scd_algorithm(df: pl.LazyFrame) -> pl.LazyFrame:
    """Apply the SCD (Severe Chronic Disease) algorithm."""
    log("Applying SCD algorithm")
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
        ),
    )

    # Add first SCD diagnosis date
    return df_with_scd.with_columns(
        first_scd_date=pl.when(pl.col("is_scd"))
        .then(pl.col("D_INDDTO"))
        .otherwise(None)
        .first()
        .over("PNR"),
    )



def add_icd_descriptions(df: pl.LazyFrame, icd_descriptions: pl.LazyFrame) -> pl.LazyFrame:
    """Add ICD-10 descriptions to the dataframe."""
    return (
        df.with_columns(
            [
                pl.col("C_ADIAG").str.slice(1).alias("icd_code_adiag"),
                pl.col("C_DIAG").str.slice(1).alias("icd_code_diag"),
            ],
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
