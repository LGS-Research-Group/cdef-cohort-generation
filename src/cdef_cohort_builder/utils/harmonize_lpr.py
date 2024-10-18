import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.utils.date import parse_dates


def integrate_lpr2_components(
    lpr_adm: pl.LazyFrame, lpr_diag: pl.LazyFrame, lpr_bes: pl.LazyFrame
) -> pl.LazyFrame:
    """Integrate LPR2 components: adm, diag, and bes."""
    logger.debug("Starting LPR2 component integration")
    logger.debug(f"LPR2 ADM schema: {lpr_adm.collect_schema()}")
    logger.debug(f"LPR2 DIAG schema: {lpr_diag.collect_schema()}")
    logger.debug(f"LPR2 BES schema: {lpr_bes.collect_schema()}")

    lpr2_integrated = lpr_adm.join(lpr_diag, left_on="RECNUM", right_on="RECNUM", how="left")
    logger.debug(f"LPR2 ADM-DIAG joined schema: {lpr2_integrated.collect_schema()}")

    lpr2_integrated = lpr2_integrated.join(lpr_bes, left_on="RECNUM", right_on="RECNUM", how="left")
    logger.debug(f"LPR2 final integrated schema: {lpr2_integrated.collect_schema()}")

    return lpr2_integrated


def integrate_lpr3_components(
    lpr3_kontakter: pl.LazyFrame, lpr3_diagnoser: pl.LazyFrame
) -> pl.LazyFrame:
    """Integrate LPR3 components: kontakter and diagnoser."""
    logger.debug("Starting LPR3 component integration")
    logger.debug(f"LPR3 kontakter schema: {lpr3_kontakter.collect_schema()}")
    logger.debug(f"LPR3 diagnoser schema: {lpr3_diagnoser.collect_schema()}")

    lpr3_integrated = lpr3_kontakter.join(
        lpr3_diagnoser, left_on="DW_EK_KONTAKT", right_on="DW_EK_KONTAKT", how="left"
    )
    logger.debug(f"LPR3 integrated schema: {lpr3_integrated.collect_schema()}")

    return lpr3_integrated


def harmonize_health_data(
    df1: pl.LazyFrame, df2: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Harmonize column names of two health data dataframes (LPR2 and LPR3)."""
    logger.debug("Starting health data harmonization")
    logger.debug(f"Input DF1 (LPR2) schema: {df1.collect_schema()}")
    logger.debug(f"Input DF2 (LPR3) schema: {df2.collect_schema()}")

    column_mappings = {
        # Patient identifier
        "PNR": "patient_id",  # LPR2
        "CPR": "patient_id",  # LPR3
        # Diagnosis
        "C_ADIAG": "primary_diagnosis",  # LPR2
        "aktionsdiagnose": "primary_diagnosis",  # LPR3
        "C_DIAG": "diagnosis",  # LPR2 (from LPR_DIAG)
        "diagnosekode": "diagnosis",  # LPR3
        "C_TILDIAG": "secondary_diagnosis",  # LPR2
        # Dates
        "D_INDDTO": "admission_date",  # LPR2
        "dato_start": "admission_date",  # LPR3
        "D_UDDTO": "discharge_date",  # LPR2
        "dato_slut": "discharge_date",  # LPR3
        "D_AMBDTO": "outpatient_date",  # LPR2 (from LPR_BES)
        # Hospital and department
        "C_SGH": "hospital_code",  # LPR2
        "SORENHED_ANS": "hospital_code",  # LPR3 (assuming this is the responsible hospital)
        "C_AFD": "department_code",  # LPR2
        # Patient type and contact type
        "C_PATTYPE": "patient_type",  # LPR2
        "kontakttype": "patient_type",  # LPR3
        # Record identifier
        "RECNUM": "record_id",  # LPR2
        "DW_EK_KONTAKT": "record_id",  # LPR3
        # Additional fields
        "C_INDM": "admission_type",  # LPR2
        "prioritet": "admission_type",  # LPR3
        "C_UDM": "discharge_type",  # LPR2
        "C_SPEC": "specialty_code",  # LPR2
        "V_SENGDAGE": "bed_days",  # LPR2
        # LPR3 specific fields
        "diagnosetype": "diagnosis_type",
        "senere_afkraeftet": "diagnosis_later_disproved",
        "kontaktaarsag": "contact_reason",
        "henvisningsaarsag": "referral_reason",
        "henvisningsmaade": "referral_method",
    }

    def rename_columns(df: pl.LazyFrame) -> pl.LazyFrame:
        for old_name, new_name in column_mappings.items():
            if old_name in df.collect_schema().names():
                df = df.rename({old_name: new_name})
                logger.debug(f"Renamed column '{old_name}' to '{new_name}'")
        return df

    df1_harmonized = rename_columns(df1)
    df2_harmonized = rename_columns(df2)

    df1_harmonized = df1_harmonized.with_columns(pl.lit("LPR2").alias("source"))
    df2_harmonized = df2_harmonized.with_columns(pl.lit("LPR3").alias("source"))

    logger.debug(f"Harmonized DF1 (LPR2) schema: {df1_harmonized.collect_schema()}")
    logger.debug(f"Harmonized DF2 (LPR3) schema: {df2_harmonized.collect_schema()}")

    return df1_harmonized, df2_harmonized


def combine_harmonized_data(df1: pl.LazyFrame, df2: pl.LazyFrame) -> pl.LazyFrame:
    """Combine the harmonized LPR2 and LPR3 dataframes."""
    logger.debug("Starting combination of harmonized data")
    logger.debug(f"Input DF1 schema: {df1.collect_schema()}")
    logger.debug(f"Input DF2 schema: {df2.collect_schema()}")

    all_columns = set(df1.collect_schema().names()).union(set(df2.collect_schema().names()))
    logger.debug(f"All unique columns: {all_columns}")

    for col in all_columns:
        if col not in df1.collect_schema().names():
            df1 = df1.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            logger.debug(f"Added missing column '{col}' to DF1")
        else:
            df1 = df1.with_columns(pl.col(col).cast(pl.Utf8))

        if col not in df2.collect_schema().names():
            df2 = df2.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            logger.debug(f"Added missing column '{col}' to DF2")
        else:
            df2 = df2.with_columns(pl.col(col).cast(pl.Utf8))

    df1 = df1.select(sorted(all_columns))
    df2 = df2.select(sorted(all_columns))

    combined_df = pl.concat([df1, df2])
    logger.debug(f"Combined dataframe schema: {combined_df.collect_schema()}")

    date_columns = ["admission_date", "outpatient_date"]
    for col in date_columns:
        if col in combined_df.collect_schema().names():
            combined_df = combined_df.with_columns(parse_dates(col).alias(col))
            logger.debug(f"Parsed dates for column '{col}'")

    logger.debug("Finished combining harmonized data")
    return combined_df


# You might want to add a function to test the harmonization process
def test_harmonization() -> None:
    logger.debug("Starting harmonization test")

    # Create sample dataframes for LPR2 and LPR3
    lpr2_sample = pl.DataFrame(
        {"PNR": ["1", "2"], "C_ADIAG": ["A", "B"], "D_INDDTO": ["2021-01-01", "2021-01-02"]}
    ).lazy()

    lpr3_sample = pl.DataFrame(
        {
            "CPR": ["3", "4"],
            "aktionsdiagnose": ["C", "D"],
            "dato_start": ["2021-01-03", "2021-01-04"],
        }
    ).lazy()

    logger.debug("Sample dataframes created")

    # Harmonize the data
    harmonized_lpr2, harmonized_lpr3 = harmonize_health_data(lpr2_sample, lpr3_sample)

    # Combine the harmonized data
    combined = combine_harmonized_data(harmonized_lpr2, harmonized_lpr3)

    logger.debug(f"Test result schema: {combined.collect_schema()}")
    logger.debug(f"Test result data:\n{combined.collect()}")


# Example usage of test function
if __name__ == "__main__":
    test_harmonization()
