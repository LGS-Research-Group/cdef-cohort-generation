import polars as pl

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.utils.config import ICD_FILE


def harmonize_health_data(
    df1: pl.LazyFrame, df2: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Harmonize column names of two health data dataframes.

    Args:
    df1 (pl.LazyFrame): First dataframe
    df2 (pl.LazyFrame): Second dataframe

    Returns:
    Tuple[pl.LazyFrame, pl.LazyFrame]: Harmonized dataframes
    """

    # Define mappings for column names
    column_mappings: dict[str, str] = {
        # Patient ID
        "PNR": "patient_id",
        "CPR": "patient_id",
        # Diagnosis codes
        "C_ADIAG": "primary_diagnosis",
        "C_DIAG": "diagnosis",
        "diagnosekode": "diagnosis",
        "aktionsdiagnose": "primary_diagnosis",
        # Dates
        "D_INDDTO": "contact_date",
        "D_AMBDTO": "contact_date",
        "dato_start": "contact_date",
        # Other potentially useful columns
        "C_DIAGTYPE": "diagnosis_type",
        "diagnosetype": "diagnosis_type",
        "RECNUM": "record_id",
        "DW_EK_KONTAKT": "contact_id",
    }

    # List of essential columns to keep
    essential_columns: list[str] = [
        "patient_id",
        "primary_diagnosis",
        "diagnosis",
        "contact_date",
        "diagnosis_type",
        "record_id",
        "contact_id",
    ]

    def rename_and_select(df: pl.LazyFrame) -> pl.LazyFrame:
        # Rename columns based on mapping
        for old_name, new_name in column_mappings.items():
            if old_name in df.collect_schema().names():
                df = df.rename({old_name: new_name})

        # Select only essential columns that exist in the dataframe
        existing_columns = [col for col in essential_columns if col in df.collect_schema().names()]
        return df.select(existing_columns)

    # Apply renaming and selection to both dataframes
    df1_harmonized = rename_and_select(df1)
    df2_harmonized = rename_and_select(df2)

    return df1_harmonized, df2_harmonized


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
            pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 4).is_in(icd_prefixes)
            | (
                (pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 4) >= "E74")
                & (pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 4) <= "E84")
            )
            | pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 5).is_in(specific_codes)
            | (
                (pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 5) >= "P941")
                & (pl.col("primary_diagnosis").str.to_uppercase().str.slice(1, 5) <= "P949")
            )
            | pl.col("diagnosis").str.to_uppercase().str.slice(1, 4).is_in(icd_prefixes)
            | (
                (pl.col("diagnosis").str.to_uppercase().str.slice(1, 4) >= "E74")
                & (pl.col("diagnosis").str.to_uppercase().str.slice(1, 4) <= "E84")
            )
            | pl.col("diagnosis").str.to_uppercase().str.slice(1, 5).is_in(specific_codes)
            | (
                (pl.col("diagnosis").str.to_uppercase().str.slice(1, 5) >= "P941")
                & (pl.col("diagnosis").str.to_uppercase().str.slice(1, 5) <= "P949")
            )
        ),
    )

    # Add first SCD diagnosis date
    return df_with_scd.with_columns(
        first_scd_date=pl.when(pl.col("is_scd"))
        .then(pl.col("contact_date"))
        .otherwise(None)
        .first()
        .over("patient_id"),
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
