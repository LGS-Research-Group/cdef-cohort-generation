import polars as pl

from cdef_cohort_generation.utils.config import ICD_FILE


def read_icd_descriptions() -> pl.LazyFrame:
    """Read ICD-10 code descriptions."""
    return pl.scan_csv(ICD_FILE)


# def apply_scd_algorithm(df: pl.LazyFrame, columns_to_check: list[str]) -> pl.LazyFrame:
#     """Apply the SCD (Severe Chronic Disease) algorithm."""
#     log("Applying SCD algorithm")
#     icd_prefixes = [
#         "C",
#         "D61",
#         "D76",
#         "D8",
#         "E10",
#         "E25",
#         "E7",
#         "G12",
#         "G31",
#         "G37",
#         "G40",
#         "G60",
#         "G70",
#         "G71",
#         "G73",
#         "G80",
#         "G81",
#         "G82",
#         "G91",
#         "G94",
#         "I12",
#         "I27",
#         "I3",
#         "I4",
#         "I5",
#         "J44",
#         "J84",
#         "K21",
#         "K5",
#         "K7",
#         "K90",
#         "M3",
#         "N0",
#         "N13",
#         "N18",
#         "N19",
#         "N25",
#         "N26",
#         "N27",
#         "P27",
#         "P57",
#         "P91",
#         "Q0",
#         "Q2",
#         "Q3",
#         "Q4",
#         "Q6",
#         "Q79",
#         "Q86",
#         "Q87",
#         "Q9",
#     ]
#     specific_codes = [
#         "D610",
#         "D613",
#         "D618",
#         "D619",
#         "D762",
#         "E730",
#         "G310",
#         "G318",
#         "G319",
#         "G702",
#         "G710",
#         "G711",
#         "G712",
#         "G713",
#         "G736",
#         "G811",
#         "G821",
#         "G824",
#         "G941",
#         "J448",
#         "P910",
#         "P911",
#         "P912",
#         "Q790",
#         "Q792",
#         "Q793",
#         "Q860",
#     ]

#     scd_conditions = []
#     for column in columns_to_check:
#         scd_conditions.extend(
#             [
#                 pl.col(column).str.to_uppercase().str.slice(1, 4).is_in(icd_prefixes),
#                 (pl.col(column).str.to_uppercase().str.slice(1, 4) >= "E74")
#                 & (pl.col(column).str.to_uppercase().str.slice(1, 4) <= "E84"),
#                 pl.col(column).str.to_uppercase().str.slice(1, 5).is_in(specific_codes),
#                 (pl.col(column).str.to_uppercase().str.slice(1, 5) >= "P941")
#                 & (pl.col(column).str.to_uppercase().str.slice(1, 5) <= "P949"),
#             ]
#         )

#     df_with_scd = df.with_columns(is_scd=pl.any_horizontal(scd_conditions))

#     # Log the number of SCD cases found
#     scd_count = df_with_scd.filter(pl.col("is_scd")).select(pl.count()).collect()[0, 0]
#     log(f"Number of SCD cases found: {scd_count}")

#     # Add first SCD diagnosis date
#     result = df_with_scd.with_columns(
#         first_scd_date=pl.when(pl.col("is_scd"))
#         .then(pl.col("contact_date"))
#         .otherwise(None)
#         .first()
#         .over("patient_id"),
#     )

#     # Log a sample of SCD cases
#     scd_sample = result.filter(pl.col("is_scd")).head(5).collect()
#     log(f"Sample of SCD cases:\n{scd_sample}")

#     return result


def apply_scd_algorithm(df: pl.LazyFrame, diagnosis_date_mapping: dict[str, str]) -> pl.LazyFrame:
    """
    Apply the Severe Chronic Disease (SCD) algorithm to the health data.

    Args:
    df (pl.LazyFrame):
        The health data LazyFrame
    diagnosis_date_mapping (dict[str, str]):
        A dictionary mapping diagnosis column names to their corresponding date column names

    Returns:
    pl.LazyFrame: The input LazyFrame with additional columns for SCD status and first SCD date
    """
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

    is_scd_expr = pl.lit(False)

    # Check for missing diagnosis columns
    missing_columns = [
        col for col in diagnosis_date_mapping.keys() if col not in df.collect_schema().names()
    ]
    if missing_columns:
        print(f"Warning: The diagnosis columns are not found: {', '.join(missing_columns)}")

    # Filter out missing columns
    valid_mapping = {
        k: v for k, v in diagnosis_date_mapping.items() if k in df.collect_schema().names()
    }

    for diag_col in valid_mapping.keys():
        is_scd_expr = is_scd_expr | (
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

    result = df.with_columns(is_scd=is_scd_expr)

    # Determine the first SCD date
    valid_date_columns = [
        col for col in valid_mapping.values() if col in df.collect_schema().names()
    ]
    if not valid_date_columns:
        print("Warning: No valid date columns found for SCD date determination")
        first_scd_date_expr = pl.lit(None).alias("first_scd_date")
    else:
        first_scd_date_expr = (
            pl.when(pl.col("is_scd"))
            .then(pl.coalesce(*[pl.col(date_col) for date_col in valid_date_columns]))
            .alias("first_scd_date")
        )

    result = result.with_columns(first_scd_date_expr)

    # Log some information about the results
    scd_count = result.filter(pl.col("is_scd")).select(pl.count()).collect()[0, 0]
    print(f"Number of SCD cases found: {scd_count}")

    # Sample of SCD cases
    scd_sample = result.filter(pl.col("is_scd")).head(5).collect()
    print(f"Sample of SCD cases:\n{scd_sample}")

    return result


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
