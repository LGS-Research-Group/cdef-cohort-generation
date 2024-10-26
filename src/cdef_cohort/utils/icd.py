import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.utils.config import ICD_FILE


def read_icd_descriptions() -> pl.LazyFrame:
    """
    Read ICD-10 code descriptions from a CSV file.

    Returns:
        pl.LazyFrame: A LazyFrame containing ICD-10 codes and their descriptions.

    """
    logger.debug(f"Reading ICD-10 descriptions from file: {ICD_FILE}")
    df = pl.scan_csv(ICD_FILE)
    logger.debug(f"ICD-10 descriptions schema: {df.collect_schema()}")
    logger.debug(f"Number of ICD-10 descriptions loaded: {df.collect().shape[0]}")
    return df


def apply_scd_algorithm_single(
    df: pl.LazyFrame, diagnosis_columns: list[str], date_column: str, patient_id_column: str
) -> pl.LazyFrame:
    """
    Apply the Severe Chronic Disease (SCD) algorithm to the health data.

    Args:
        df (pl.LazyFrame): The health data LazyFrame.
        diagnosis_columns (list[str]): A list of column names containing diagnosis codes.
        date_column (str): The name of the column containing dates.
        patient_id_column (str): The name of the column containing patient IDs.

    Returns:
        pl.LazyFrame: A LazyFrame with SCD flags and dates aggregated at the patient level.
    """
    logger.debug(f"Applying SCD algorithm with diagnosis columns: {diagnosis_columns}")
    logger.debug(f"Date column: {date_column}, Patient ID column: {patient_id_column}")
    scd_codes = [
        "D55",
        "D56",
        "D57",
        "D58",
        "D60",
        "D61",
        "D64",
        "D66",
        "D67",
        "D68",
        "D69",
        "D70",
        "D71",
        "D72",
        "D73",
        "D76",
        "D80",
        "D81",
        "D82",
        "D83",
        "D84",
        "D86",
        "D89",
        "E22",
        "E23",
        "E24",
        "E25",
        "E26",
        "E27",
        "E31",
        "E34",
        "E70",
        "E71",
        "E72",
        "E73",
        "E74",
        "E75",
        "E76",
        "E77",
        "E78",
        "E79",
        "E80",
        "E83",
        "E84",
        "E85",
        "E88",
        "F84",
        "G11",
        "G12",
        "G13",
        "G23",
        "G24",
        "G25",
        "G31",
        "G32",
        "G36",
        "G37",
        "G40",
        "G41",
        "G60",
        "G70",
        "G71",
        "G72",
        "G73",
        "G80",
        "G81",
        "G82",
        "G83",
        "G90",
        "G91",
        "G93",
        "I27",
        "I42",
        "I43",
        "I50",
        "I61",
        "I63",
        "I69",
        "I70",
        "I71",
        "I72",
        "I73",
        "I74",
        "I77",
        "I78",
        "I79",
        "J41",
        "J42",
        "J43",
        "J44",
        "J45",
        "J47",
        "J60",
        "J61",
        "J62",
        "J63",
        "J64",
        "J65",
        "J66",
        "J67",
        "J68",
        "J69",
        "J70",
        "J84",
        "J98",
        "K50",
        "K51",
        "K73",
        "K74",
        "K86",
        "K87",
        "K90",
        "M05",
        "M06",
        "M07",
        "M08",
        "M09",
        "M30",
        "M31",
        "M32",
        "M33",
        "M34",
        "M35",
        "M40",
        "M41",
        "M42",
        "M43",
        "M45",
        "M46",
        "N01",
        "N03",
        "N04",
        "N07",
        "N08",
        "N11",
        "N12",
        "N13",
        "N14",
        "N15",
        "N16",
        "N18",
        "N19",
        "N20",
        "N21",
        "N22",
        "N23",
        "N25",
        "N26",
        "N27",
        "N28",
        "N29",
        "P27",
        "Q01",
        "Q02",
        "Q03",
        "Q04",
        "Q05",
        "Q06",
        "Q07",
        "Q20",
        "Q21",
        "Q22",
        "Q23",
        "Q24",
        "Q25",
        "Q26",
        "Q27",
        "Q28",
        "Q30",
        "Q31",
        "Q32",
        "Q33",
        "Q34",
        "Q35",
        "Q36",
        "Q37",
        "Q38",
        "Q39",
        "Q40",
        "Q41",
        "Q42",
        "Q43",
        "Q44",
        "Q45",
        "Q60",
        "Q61",
        "Q62",
        "Q63",
        "Q64",
        "Q65",
        "Q66",
        "Q67",
        "Q68",
        "Q69",
        "Q70",
        "Q71",
        "Q72",
        "Q73",
        "Q74",
        "Q75",
        "Q76",
        "Q77",
        "Q78",
        "Q79",
        "Q80",
        "Q81",
        "Q82",
        "Q83",
        "Q84",
        "Q85",
        "Q86",
        "Q87",
        "Q89",
        "Q90",
        "Q91",
        "Q92",
        "Q93",
        "Q95",
        "Q96",
        "Q97",
        "Q98",
        "Q99",
    ]
    logger.debug(f"Number of SCD codes: {len(scd_codes)}")
    scd_conditions = []
    for diag_col in diagnosis_columns:
        scd_condition = (
            pl.col(diag_col).str.to_uppercase().str.slice(1, 4).is_in(scd_codes)
            | pl.col(diag_col).str.to_uppercase().str.slice(1, 5).is_in(scd_codes)
            | (
                (pl.col(diag_col).str.to_uppercase().str.slice(1, 4) >= pl.lit("E74"))
                & (pl.col(diag_col).str.to_uppercase().str.slice(1, 4) <= pl.lit("E84"))
            )
            | (
                (pl.col(diag_col).str.to_uppercase().str.slice(1, 5) >= pl.lit("P941"))
                & (pl.col(diag_col).str.to_uppercase().str.slice(1, 5) <= pl.lit("P949"))
            )
        )
        scd_conditions.append(scd_condition)

    logger.debug(f"Number of SCD conditions created: {len(scd_conditions)}")
    is_scd_expr = pl.any_horizontal(*scd_conditions)

    result = df.with_columns(
        [
            is_scd_expr.alias("is_scd"),
            pl.when(is_scd_expr).then(pl.col(date_column)).otherwise(None).alias("first_scd_date"),
        ]
    )
    logger.debug("SCD conditions applied to dataframe")

    # Aggregate to patient level
    aggregated = result.group_by(patient_id_column).agg(
        [
            pl.col("is_scd").max().alias("is_scd"),
            pl.col("first_scd_date").min().alias("first_scd_date"),
        ]
    )

    logger.debug("Aggregated results to patient level")
    logger.debug(f"Aggregated schema: {aggregated.collect_schema()}")

    return aggregated


def add_icd_descriptions(df: pl.LazyFrame, icd_descriptions: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add ICD-10 descriptions to the dataframe.

    Args:
        df (pl.LazyFrame): The input LazyFrame containing ICD codes.
        icd_descriptions (pl.LazyFrame): A LazyFrame with ICD-10 codes and their descriptions.

    Returns:
        pl.LazyFrame:
            A LazyFrame with added ICD-10 descriptions for both admission and main diagnoses.
    """
    logger.debug("Adding ICD-10 descriptions to dataframe")
    logger.debug(f"Input dataframe schema: {df.collect_schema()}")
    logger.debug(f"ICD descriptions schema: {icd_descriptions.collect_schema()}")

    result = (
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

    logger.debug(f"Result schema after adding ICD descriptions: {result.collect_schema()}")
    logger.debug(f"Number of rows in result: {result.collect().shape[0]}")

    return result


# You might want to add a function to test the ICD utilities
def test_icd_utils() -> None:
    """
    Test the ICD utilities functions.

    This function runs a series of tests on the ICD utility functions:
    - Tests read_icd_descriptions()
    - Tests apply_scd_algorithm_single()
    - Tests add_icd_descriptions()

    """
    logger.debug("Starting ICD utilities test")

    # Test read_icd_descriptions
    icd_descriptions = read_icd_descriptions()
    logger.debug(f"ICD descriptions sample: {icd_descriptions.collect().head(5)}")

    # Create a sample dataframe for testing SCD algorithm
    sample_df = pl.DataFrame(
        {
            "patient_id": ["1", "2", "3"],
            "diagnosis": ["A001", "E750", "Z000"],
            "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
        }
    ).lazy()

    # Test apply_scd_algorithm_single
    scd_result = apply_scd_algorithm_single(sample_df, ["diagnosis"], "date", "patient_id")
    logger.debug(f"SCD algorithm result: {scd_result.collect()}")

    # Test add_icd_descriptions
    sample_df_with_codes = pl.DataFrame(
        {"C_ADIAG": ["A001", "E750", "Z000"], "C_DIAG": ["B001", "F500", "Y000"]}
    ).lazy()
    described_df = add_icd_descriptions(sample_df_with_codes, icd_descriptions)
    logger.debug(f"Dataframe with ICD descriptions: {described_df.collect().head()}")

    logger.debug("Finished ICD utilities test")


# Example usage of test function
if __name__ == "__main__":
    test_icd_utils()
