from pathlib import Path

import polars as pl

from cdef_cohort_generation.utils import parse_dates

# Constants
DATA_DIR = Path("E:/workdata/708245/data/register")
COHORT_FILE = Path("data/02_education/cohort.parquet")
OUTPUT_FILE = Path("data/03_lpr/cohort.parquet")
ICD_FILE = Path("data/icd10dict.csv")
PARQUET = "*.parquet"

LPR_ADM_SCHEMA = {
    "PNR": pl.Utf8,
    "C_ADIAG": pl.Utf8,
    "C_AFD": pl.Utf8,
    "C_HAFD": pl.Utf8,
    "C_HENM": pl.Utf8,
    "C_HSGH": pl.Utf8,
    "C_INDM": pl.Utf8,
    "C_KOM": pl.Utf8,
    "C_KONTAARS": pl.Utf8,
    "C_PATTYPE": pl.Utf8,
    "C_SGH": pl.Utf8,
    "C_SPEC": pl.Utf8,
    "C_UDM": pl.Utf8,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "D_HENDTO": pl.Date,
    "D_INDDTO": pl.Date,
    "D_UDDTO": pl.Date,
    "K_AFD": pl.Utf8,
    "RECNUM": pl.Utf8,
    "V_ALDDG": pl.Int32,
    "V_ALDER": pl.Int32,
    "V_INDMINUT": pl.Int32,
    "V_INDTIME": pl.Int32,
    "V_SENGDAGE": pl.Int32,
    "V_UDTIME": pl.Int32,
    "VERSION": pl.Utf8,
}

LPR_DIAG_SCHEMA = {
    "C_DIAG": pl.Utf8,
    "C_DIAGMOD": pl.Utf8,
    "C_DIAGTYPE": pl.Utf8,
    "C_TILDIAG": pl.Utf8,
    "LEVERANCEDATO": pl.Date,
    "RECNUM": pl.Utf8,
    "VERSION": pl.Utf8,
}

LPR3_DIAGNOSER_SCHEMA = {
    "DW_EK_KONTAKT": pl.Utf8,
    "diagnosekode": pl.Utf8,
    "diagnosetype": pl.Utf8,
    "senere_afkraeftet": pl.Utf8,
    "diagnosekode_parent": pl.Utf8,
    "diagnosetype_parent": pl.Utf8,
    "lprindberetningssystem": pl.Utf8,
}

LPR3_KONTAKTER_SCHEMA = {
    "SORENHED_IND": pl.Utf8,
    "SORENHED_HEN": pl.Utf8,
    "SORENHED_ANS": pl.Utf8,
    "DW_EK_KONTAKT": pl.Utf8,
    "DW_EK_FORLOEB": pl.Utf8,
    "CPR": pl.Utf8,
    "dato_start": pl.Date,
    "tidspunkt_start": pl.Time,
    "dato_slut": pl.Date,
    "tidspunkt_slut": pl.Time,
    "aktionsdiagnose": pl.Utf8,
    "kontaktaarsag": pl.Utf8,
    "prioritet": pl.Utf8,
    "kontakttype": pl.Utf8,
    "henvisningsaarsag": pl.Utf8,
    "henvisningsmaade": pl.Utf8,
    "dato_behandling_start": pl.Date,
    "tidspunkt_behandling_start": pl.Time,
    "dato_indberetning": pl.Date,
    "lprindberetningssytem": pl.Utf8,
}


def read_cohort_data() -> pl.DataFrame:
    """Read cohort data from parquet file."""
    return pl.read_parquet(COHORT_FILE)


def process_lpr_adm_file(file_path: Path) -> pl.DataFrame:
    """Process a single LPR2 ADM parquet file."""
    return pl.read_parquet(file_path, columns=list(LPR_ADM_SCHEMA.keys())).with_columns(
        [
            parse_dates("D_HENDTO").alias("D_HENDTO"),
            parse_dates("D_INDDTO").alias("D_INDDTO"),
            parse_dates("D_UDDTO").alias("D_UDDTO"),
        ]
    )


def process_lpr_diag_file(file_path: Path) -> pl.DataFrame:
    """Process a single LPR2 DIAG parquet file."""
    return pl.read_parquet(file_path, columns=list(LPR_DIAG_SCHEMA.keys())).with_columns(
        [parse_dates("LEVERANCEDATO").alias("LEVERANCEDATO")]
    )


def process_lpr3_diagnoser_file(file_path: Path) -> pl.DataFrame:
    """Process a single LPR3 DIAGNOSER parquet file."""
    return pl.read_parquet(file_path, columns=list(LPR3_DIAGNOSER_SCHEMA.keys()))


def process_lpr3_kontakter_file(file_path: Path) -> pl.DataFrame:
    """Process a single LPR3 KONTAKTER parquet file."""
    return pl.read_parquet(file_path, columns=list(LPR3_KONTAKTER_SCHEMA.keys())).with_columns(
        [
            parse_dates("dato_start").alias("dato_start"),
            parse_dates("dato_slut").alias("dato_slut"),
            parse_dates("dato_behandling_start").alias("dato_behandling_start"),
            parse_dates("dato_indberetning").alias("dato_indberetning"),
        ]
    )


def read_health_data() -> pl.DataFrame:
    """Read and process all LPR2 and LPR3 parquet files."""
    lpr_adm = pl.concat([process_lpr_adm_file(f) for f in (DATA_DIR / "lpr_adm").glob(PARQUET)])
    lpr_diag = pl.concat([process_lpr_diag_file(f) for f in (DATA_DIR / "lpr_diag").glob(PARQUET)])

    # LPR3 files are not present in the provided directory structure
    lpr3_diagnoser = pl.concat(
        [process_lpr3_diagnoser_file(f) for f in (DATA_DIR / "diagnoser").glob(PARQUET)]
    )
    lpr3_kontakter = pl.concat(
        [process_lpr3_kontakter_file(f) for f in (DATA_DIR / "kontakter").glob(PARQUET)]
    )

    # Combine LPR2 ADM and DIAG
    lpr2 = lpr_adm.join(lpr_diag, on="RECNUM", how="left")

    # Combine LPR3 DIAGNOSER and KONTAKTER
    lpr3 = lpr3_kontakter.join(lpr3_diagnoser, on="DW_EK_KONTAKT", how="left")
    lpr3 = lpr3.with_columns(
        [
            pl.col("diagnosekode").alias("C_DIAG"),
            pl.col("aktionsdiagnose").alias("C_ADIAG"),
            pl.col("CPR").alias("PNR"),
            pl.col("dato_start").alias("D_INDDTO"),
            pl.col("dato_slut").alias("D_UDDTO"),
            pl.lit("3").alias("LPR_VERSION"),
        ]
    )

    # Combine LPR2 and LPR3 data
    combined = lpr2.with_columns(pl.lit("2").alias("LPR_VERSION"))

    return combined.sort(["PNR", "D_INDDTO"])


def link_cohort_with_health_data(
    cohort_df: pl.DataFrame, health_data: pl.DataFrame
) -> pl.DataFrame:
    """Link cohort data with health data."""
    return cohort_df.join(health_data, on="PNR", how="inner").sort(["PNR", "D_INDDTO"])


def read_icd_descriptions() -> pl.DataFrame:
    """Read ICD-10 code descriptions."""
    return pl.read_csv(ICD_FILE)


def apply_scd_algorithm(df: pl.DataFrame) -> pl.DataFrame:
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


def add_icd_descriptions(df: pl.DataFrame, icd_descriptions: pl.DataFrame) -> pl.DataFrame:
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


def main() -> None:
    # Read cohort data
    cohort_df = read_cohort_data()

    # Read and process health data
    health_data = read_health_data()

    # Link cohort with health data
    linked_data = link_cohort_with_health_data(cohort_df, health_data)

    # Apply SCD algorithm
    scd_data = apply_scd_algorithm(linked_data)
    scd_data = scd_data.with_columns(
        pre_exposure_start=pl.col("first_scd_date").dt.offset_by("-1y"),
        pre_exposure_end=pl.col("first_scd_date"),
    )

    # Read ICD descriptions and add to the data
    icd_descriptions = read_icd_descriptions()
    final_data = add_icd_descriptions(scd_data, icd_descriptions)

    # Write output to file
    final_data.write_parquet(OUTPUT_FILE)
    print(f"Data written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
