import os
from pathlib import Path

import polars as pl

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.population import main as generate_population
from cdef_cohort_generation.registers import (
    process_akm,
    process_bef,
    process_idan,
    process_ind,
    process_lpr3_diagnoser,
    process_lpr3_kontakter,
    process_lpr_adm,
    process_lpr_bes,
    process_lpr_diag,
    process_uddf,
)
from cdef_cohort_generation.utils import (
    AKM_OUT,
    BEF_OUT,
    COHORT_FILE,
    EVENT_DEFINITIONS,
    IDAN_OUT,
    IND_OUT,
    LPR3_DIAGNOSER_OUT,
    LPR3_KONTAKTER_OUT,
    LPR_ADM_OUT,
    LPR_BES_OUT,
    LPR_DIAG_OUT,
    POPULATION_FILE,
    STATIC_COHORT,
    UDDF_OUT,
    apply_scd_algorithm,
    combine_harmonized_data,
    harmonize_health_data,
    identify_events,
    integrate_lpr2_components,
    integrate_lpr3_components,
)


def log_lazyframe_info(name: str, df: pl.LazyFrame) -> None:
    schema = df.collect_schema()
    total_rows = df.select(pl.count()).collect()[0, 0]

    log(f"--- {name} ---")
    log(f"Number of rows: {total_rows}")
    log(f"Columns and types: {str(schema)}")

    # Compute missing values for each column
    missing_values = df.select(
        [pl.col(col).null_count().alias(f"{col}_null_count") for col in schema.keys()]
    ).collect()

    log("Missing values:")
    for col in schema.keys():
        null_count = missing_values[0, f"{col}_null_count"]
        percentage = (null_count / total_rows) * 100
        log(f"  {col}: {null_count} ({percentage:.2f}%)")

    # Fetch a sample of data, handling the case where 'month' might not be present
    sample_data = df.fetch(5)
    log(f"Sample data:\n{sample_data}")
    log("-------------------")


def identify_severe_chronic_disease() -> pl.LazyFrame:
    """Process health data and identify children with severe chronic diseases.

    Returns:
    pl.DataFrame: DataFrame with PNR, is_scd flag, and first_scd_date.

    """
    # Step 1: Process health register data
    process_lpr_adm(columns_to_keep=["PNR", "C_ADIAG", "RECNUM", "D_INDDTO"])
    process_lpr_diag(columns_to_keep=["RECNUM", "C_DIAG", "C_TILDIAG"])
    process_lpr_bes(columns_to_keep=["D_AMBDTO", "RECNUM"])
    process_lpr3_diagnoser(columns_to_keep=["DW_EK_KONTAKT", "diagnosekode"])
    process_lpr3_kontakter(
        columns_to_keep=["DW_EK_KONTAKT", "CPR", "aktionsdiagnose", "dato_start"]
    )

    # Read processed health data
    # LPR2
    lpr_adm = pl.scan_parquet(LPR_ADM_OUT)
    lpr_diag = pl.scan_parquet(LPR_DIAG_OUT)
    lpr_bes = pl.scan_parquet(LPR_BES_OUT)
    # LPR3
    lpr3_diagnoser = pl.scan_parquet(LPR3_DIAGNOSER_OUT)
    lpr3_kontakter = pl.scan_parquet(LPR3_KONTAKTER_OUT)

    # Log info for individual LazyFrames
    # log_lazyframe_info("LPR_ADM", lpr_adm)
    # log_lazyframe_info("LPR_DIAG", lpr_diag)
    # log_lazyframe_info("LPR_BES", lpr_bes)
    # log_lazyframe_info("LPR3_DIAGNOSER", lpr3_diagnoser)
    # log_lazyframe_info("LPR3_KONTAKTER", lpr3_kontakter)

    # Combine LPR2 data
    lpr2 = integrate_lpr2_components(lpr_adm, lpr_diag, lpr_bes)
    # Log info for combined LPR2 data
    # log_lazyframe_info("Combined LPR2", lpr2)

    # Combine LPR3 data
    lpr3 = integrate_lpr3_components(lpr3_kontakter, lpr3_diagnoser)
    # Log info for combined LPR3 data
    # log_lazyframe_info("Combined LPR3", lpr3)

    # Step 5: Harmonize and combine all health data
    lpr2_harmonized, lpr3_harmonized = harmonize_health_data(lpr2, lpr3)

    # Log info for harmonized data
    # log_lazyframe_info("Harmonized LPR2", lpr2_harmonized)
    # log_lazyframe_info("Harmonized LPR3", lpr3_harmonized)

    # Combine harmonized data
    health_data = combine_harmonized_data(lpr2_harmonized, lpr3_harmonized)

    # log("Combined health data schema:")
    # log(str(health_data.collect_schema()))

    # log("Sample of combined health data:")
    # sample_data = health_data.fetch(5)
    # log(str(sample_data))

    # log("Column types:")
    # for col in sample_data.columns:
    #     log(f"{col}: {sample_data[col].dtype}")

    # # Log info for final combined health data
    # log_lazyframe_info("Combined Health Data", health_data)

    # # Step 6: Apply SCD algorithm
    # columns_to_check = [
    #     "primary_diagnosis",
    #     "secondary_diagnosis",
    #     "diagnosis",
    # ]
    # scd_data = apply_scd_algorithm(health_data, columns_to_check)

    diagnosis_date_mapping = {
        "primary_diagnosis": "admission_date",
        "secondary_diagnosis": "admission_date",
        "diagnosis": "outpatient_date",
    }
    scd_data = apply_scd_algorithm(health_data, diagnosis_date_mapping)

    # Debug: Log column names after SCD algorithm
    # log(f"SCD data columns: {scd_data.collect_schema().names()}")

    # Check if patient_id exists
    if "patient_id" not in scd_data.collect_schema().names():
        raise ValueError("patient_id column not found in SCD data after processing")

    # Step 7: Aggregate to patient level
    aggregated_scd_data = scd_data.group_by("patient_id").agg(
        [
            pl.col("is_scd").max().alias("is_scd"),
            pl.col("first_scd_date").min().alias("first_scd_date"),
        ],
    )

    # Collect the data and print summary
    collected_data = aggregated_scd_data.collect()
    total_patients = collected_data.shape[0]
    scd_patients = collected_data.filter(pl.col("is_scd")).shape[0]

    log(f"Total number of patients: {total_patients}")
    log(f"Number of patients with SCD: {scd_patients}")
    log(f"Percentage of patients with SCD: {scd_patients / total_patients * 100:.2f}%")

    # Print a sample of SCD patients
    scd_sample = collected_data.filter(pl.col("is_scd")).head(n=min(5, scd_patients))
    log("Sample of SCD patients:")
    log(f"Sample of SCD cases:\n{scd_sample}")

    return aggregated_scd_data


def process_static_data(scd_data: pl.LazyFrame) -> pl.LazyFrame:
    """Process static cohort data."""
    population = pl.scan_parquet(POPULATION_FILE)

    # Check if PNR exists in both dataframes
    if "PNR" not in population.collect_schema().names():
        raise ValueError("PNR column not found in population data")
    if "patient_id" not in scd_data.collect_schema().names():
        raise ValueError("PNR column not found in SCD data")

    # Ensure PNR is of the same type in both dataframes
    population = population.with_columns(pl.col("PNR").cast(pl.Utf8))
    scd_data = scd_data.with_columns(pl.col("patient_id").cast(pl.Utf8))

    # Join the dataframes
    result = population.join(scd_data, left_on="PNR", right_on="patient_id", how="left")

    # Check if PNR exists in the result
    if "PNR" not in result.collect_schema().names():
        raise ValueError("PNR column not found in joined result")

    return result


def process_longitudinal_data() -> pl.LazyFrame:
    """Process longitudinal data from various registers."""
    common_params = {
        "population_file": STATIC_COHORT,
        "longitudinal": True,
    }

    # Process registers that contain longitudinal data
    process_bef(**common_params)
    process_akm(**common_params)
    process_ind(**common_params)
    process_idan(**common_params)
    process_uddf(**common_params)

    # Combine longitudinal data from different registers
    longitudinal_registers = [BEF_OUT, AKM_OUT, IND_OUT, IDAN_OUT, UDDF_OUT]
    longitudinal_data = []
    all_columns = set()

    for register in longitudinal_registers:
        register_data = pl.scan_parquet(register)
        log(f"Schema for {register}: {register_data.collect_schema()}")
        all_columns.update(register_data.collect_schema().names())
        longitudinal_data.append(register_data)

    log(f"All columns across registers: {all_columns}")

    # Use union_all instead of concat
    return pl.concat(longitudinal_data, how="diagonal")


# Main execution
def main(output_dir: Path | None = None) -> None:
    if output_dir is None:
        output_dir = COHORT_FILE.parent

    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(LPR_ADM_OUT.parent, exist_ok=True)
    os.makedirs(LPR_DIAG_OUT.parent, exist_ok=True)
    os.makedirs(LPR_BES_OUT.parent, exist_ok=True)
    os.makedirs(LPR3_DIAGNOSER_OUT.parent, exist_ok=True)
    os.makedirs(LPR3_KONTAKTER_OUT.parent, exist_ok=True)

    log("Starting cohort generation process")

    # Generate population
    log("Generating population data")
    generate_population()
    log("Population data generation completed")

    # Process health data and identify SCD
    log("Identifying severe chronic diseases")
    scd_data = identify_severe_chronic_disease()
    log("Severe chronic disease identification completed")

    # Process static data
    log("Processing static data")
    static_cohort = process_static_data(scd_data)
    log("Static data processing completed")
    static_cohort.collect().write_parquet(STATIC_COHORT)
    log(f"Static cohort data written to {STATIC_COHORT.name}")

    # Process longitudinal data
    log("Processing longitudinal data")
    longitudinal_data = process_longitudinal_data()
    # log_lazyframe_info("Longitudianl data: ", longitudinal_data)
    log("Longitudinal data processing completed")
    longitudinal_data.collect().write_parquet(output_dir / "longitudinal_data.parquet")
    log(f"Longitudinal data written to {output_dir / 'longitudinal_data.parquet'}")

    # Identify events
    events = identify_events(longitudinal_data, EVENT_DEFINITIONS)
    events.collect().write_parquet(output_dir / "events.parquet")


if __name__ == "__main__":
    main()
