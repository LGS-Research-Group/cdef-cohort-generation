import polars as pl

from cdef_cohort_generation.utils.date import parse_dates


def integrate_lpr2_components(
    lpr_adm: pl.LazyFrame, lpr_diag: pl.LazyFrame, lpr_bes: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Integrate LPR2 components: adm, diag, and bes.

    Args:
    lpr_adm (pl.LazyFrame): LPR2 administrative data
    lpr_diag (pl.LazyFrame): LPR2 diagnosis data
    lpr_bes (pl.LazyFrame): LPR2 outpatient visit data

    Returns:
    pl.LazyFrame: Integrated LPR2 data
    """
    # Join adm and diag on RECNUM
    lpr2_integrated = lpr_adm.join(lpr_diag, left_on="RECNUM", right_on="RECNUM", how="left")

    # Join with bes on RECNUM
    lpr2_integrated = lpr2_integrated.join(lpr_bes, left_on="RECNUM", right_on="RECNUM", how="left")

    return lpr2_integrated


def integrate_lpr3_components(
    lpr3_kontakter: pl.LazyFrame, lpr3_diagnoser: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Integrate LPR3 components: kontakter and diagnoser.

    Args:
    lpr3_kontakter (pl.LazyFrame): LPR3 contact data
    lpr3_diagnoser (pl.LazyFrame): LPR3 diagnosis data

    Returns:
    pl.LazyFrame: Integrated LPR3 data
    """
    # Join kontakter and diagnoser on DW_EK_KONTAKT
    lpr3_integrated = lpr3_kontakter.join(
        lpr3_diagnoser, left_on="DW_EK_KONTAKT", right_on="DW_EK_KONTAKT", how="left"
    )

    return lpr3_integrated


def harmonize_health_data(
    df1: pl.LazyFrame, df2: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Harmonize column names of two health data dataframes (LPR2 and LPR3).

    Args:
    df1 (pl.LazyFrame): First dataframe (LPR2)
    df2 (pl.LazyFrame): Second dataframe (LPR3)

    Returns:
    Tuple[pl.LazyFrame, pl.LazyFrame]: Harmonized dataframes
    """

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
        return df

    df1_harmonized = rename_columns(df1)
    df2_harmonized = rename_columns(df2)

    # Add a source column to identify the origin of each record
    df1_harmonized = df1_harmonized.with_columns(pl.lit("LPR2").alias("source"))
    df2_harmonized = df2_harmonized.with_columns(pl.lit("LPR3").alias("source"))

    return df1_harmonized, df2_harmonized


def combine_harmonized_data(df1: pl.LazyFrame, df2: pl.LazyFrame) -> pl.LazyFrame:
    """
    Combine the harmonized LPR2 and LPR3 dataframes.

    Args:
    df1 (pl.LazyFrame): Harmonized LPR2 dataframe
    df2 (pl.LazyFrame): Harmonized LPR3 dataframe

    Returns:
    pl.LazyFrame: Combined dataframe
    """
    # Get all unique columns from both dataframes
    all_columns = set(df1.collect_schema().names()).union(set(df2.collect_schema().names()))

    # Ensure both dataframes have all columns as strings
    for col in all_columns:
        if col not in df1.collect_schema().names():
            df1 = df1.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
        else:
            df1 = df1.with_columns(pl.col(col).cast(pl.Utf8))

        if col not in df2.collect_schema().names():
            df2 = df2.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
        else:
            df2 = df2.with_columns(pl.col(col).cast(pl.Utf8))

    # Ensure both dataframes have the same column order
    df1 = df1.select(sorted(all_columns))
    df2 = df2.select(sorted(all_columns))

    # Combine the dataframes
    combined_df = pl.concat([df1, df2])

    # Now attempt to convert date columns
    date_columns = ["admission_date", "outpatient_date"]
    for col in date_columns:
        if col in combined_df.collect_schema().names():
            combined_df = combined_df.with_columns(parse_dates(col).alias(col))
    return combined_df
