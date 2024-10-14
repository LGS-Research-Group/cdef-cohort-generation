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
    UDDF_OUT,
    apply_scd_algorithm,
    harmonize_health_data,
    identify_events,
)


def identify_severe_chronic_disease() -> pl.LazyFrame:
    """Process health data and identify children with severe chronic diseases.

    Returns:
    pl.DataFrame: DataFrame with PNR, is_scd flag, and first_scd_date.

    """
    # Step 1: Process health register data
    process_lpr_adm()
    process_lpr_diag()
    process_lpr_bes()
    process_lpr3_diagnoser()
    process_lpr3_kontakter()

    # Step 2: Read processed health data
    lpr_adm = pl.scan_parquet(LPR_ADM_OUT)
    lpr_diag = pl.scan_parquet(LPR_DIAG_OUT)
    lpr_bes = pl.scan_parquet(LPR_BES_OUT)
    lpr3_diagnoser = pl.scan_parquet(LPR3_DIAGNOSER_OUT)
    lpr3_kontakter = pl.scan_parquet(LPR3_KONTAKTER_OUT)

    # Step 3: Combine LPR2 data
    lpr2 = lpr_adm.join(lpr_diag, on="RECNUM", how="left")
    lpr2 = lpr2.join(lpr_bes, on="RECNUM", how="left")

    # Step 4: Combine LPR3 data
    lpr3 = lpr3_kontakter.join(lpr3_diagnoser, on="DW_EK_KONTAKT", how="left")

    # Debug: Log column names before harmonization
    log(f"LPR2 columns before harmonization: {lpr2.collect_schema().names()}")
    log(f"LPR3 columns before harmonization: {lpr3.collect_schema().names()}")
    # Step 5: Harmonize and combine all health data
    lpr2_harmonized, lpr3_harmonized = harmonize_health_data(lpr2, lpr3)

    log(f"LPR2 columns after harmonization: {lpr2_harmonized.collect_schema().names()}")
    log(f"LPR3 columns after harmonization: {lpr3_harmonized.collect_schema().names()}")
    health_data = pl.concat([lpr2_harmonized, lpr3_harmonized])

    # Step 6: Apply SCD algorithm
    scd_data = apply_scd_algorithm(health_data)

    # Debug: Log column names after SCD algorithm
    log(f"SCD data columns: {scd_data.collect_schema().names()}")

    # Check if patient_id exists
    if "patient_id" not in scd_data.collect_schema().names():
        raise ValueError("patient_id column not found in SCD data after processing")

    # Step 7: Aggregate to patient level
    return scd_data.group_by("patient_id").agg(
        [
            pl.col("is_scd").max().alias("is_scd"),
            pl.col("first_scd_date").min().alias("first_scd_date"),
        ],
    )


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
    # Process registers that contain longitudinal data
    process_bef(longitudinal=True)
    process_akm(longitudinal=True)
    process_ind(longitudinal=True)
    process_idan(longitudinal=True)
    process_uddf(longitudinal=True)

    # Combine longitudinal data from different registers
    longitudinal_registers = [BEF_OUT, AKM_OUT, IND_OUT, IDAN_OUT, UDDF_OUT]
    longitudinal_data = []
    for register in longitudinal_registers:
        register_data = pl.scan_parquet(register)
        longitudinal_data.append(register_data)

    return pl.concat(longitudinal_data)


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
    static_cohort.collect().write_parquet(output_dir / "static_cohort.parquet")
    log(f"Static cohort data written to {output_dir / 'static_cohort.parquet'}")

    # Process longitudinal data
    log("Processing longitudinal data")
    longitudinal_data = process_longitudinal_data()
    log("Longitudinal data processing completed")
    longitudinal_data.collect().write_parquet(output_dir / "longitudinal_data.parquet")
    log(f"Longitudinal data written to {output_dir / 'longitudinal_data.parquet'}")

    # Identify events
    events = identify_events(longitudinal_data, EVENT_DEFINITIONS)
    events.collect().write_parquet(output_dir / "events.parquet")


if __name__ == "__main__":
    main()
