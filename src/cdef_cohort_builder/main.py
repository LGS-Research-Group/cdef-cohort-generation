import os
from pathlib import Path

import polars as pl

from cdef_cohort_builder.event_summaries import main as generate_event_summaries
from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.population import main as generate_population
from cdef_cohort_builder.registers import (
    lpr3_diagnoser,
    lpr3_kontakter,
    lpr_adm,
    lpr_bes,
    lpr_diag,
)
from cdef_cohort_builder.registers.longitudinal import process_and_partition_longitudinal_data
from cdef_cohort_builder.utils.config import (
    COHORT_FILE,
    EVENT_DEFINITIONS,
    LPR3_DIAGNOSER_OUT,
    LPR3_KONTAKTER_OUT,
    LPR_ADM_OUT,
    LPR_BES_OUT,
    LPR_DIAG_OUT,
    POPULATION_FILE,
    STATIC_COHORT,
)
from cdef_cohort_builder.utils.event import identify_events
from cdef_cohort_builder.utils.harmonize_lpr import (
    integrate_lpr2_components,
    integrate_lpr3_components,
)
from cdef_cohort_builder.utils.hash_utils import process_with_hash_check
from cdef_cohort_builder.utils.icd import apply_scd_algorithm_single


def identify_severe_chronic_disease() -> pl.LazyFrame:
    """Process health data and identify children with severe chronic diseases."""
    logger.info("Starting identification of severe chronic diseases")

    logger.debug("Processing LPR_ADM data")
    process_with_hash_check(
        lpr_adm.process_lpr_adm, columns_to_keep=["PNR", "C_ADIAG", "RECNUM", "D_INDDTO"]
    )

    logger.debug("Processing LPR_DIAG data")
    process_with_hash_check(
        lpr_diag.process_lpr_diag, columns_to_keep=["RECNUM", "C_DIAG", "C_TILDIAG"]
    )

    logger.debug("Processing LPR_BES data")
    process_with_hash_check(lpr_bes.process_lpr_bes, columns_to_keep=["D_AMBDTO", "RECNUM"])

    logger.debug("Processing LPR3_DIAGNOSER data")
    process_with_hash_check(
        lpr3_diagnoser.process_lpr3_diagnoser, columns_to_keep=["DW_EK_KONTAKT", "diagnosekode"]
    )

    logger.debug("Processing LPR3_KONTAKTER data")
    process_with_hash_check(
        lpr3_kontakter.process_lpr3_kontakter,
        columns_to_keep=["DW_EK_KONTAKT", "CPR", "aktionsdiagnose", "dato_start"],
    )

    logger.info("Integrating LPR2 components")
    lpr2 = integrate_lpr2_components(
        pl.scan_parquet(LPR_ADM_OUT), pl.scan_parquet(LPR_DIAG_OUT), pl.scan_parquet(LPR_BES_OUT)
    )

    logger.debug(f"LPR2 data schema: {lpr2.collect_schema()}")

    logger.info("Applying SCD algorithm to LPR2 data")
    lpr2_scd = apply_scd_algorithm_single(
        lpr2,
        diagnosis_columns=["C_ADIAG", "C_DIAG", "C_TILDIAG"],
        date_column="D_INDDTO",
        patient_id_column="PNR",
    )

    logger.info("Integrating LPR3 components")
    lpr3 = integrate_lpr3_components(
        pl.scan_parquet(LPR3_KONTAKTER_OUT), pl.scan_parquet(LPR3_DIAGNOSER_OUT)
    )

    logger.info("Applying SCD algorithm to LPR3 data")
    lpr3_scd = apply_scd_algorithm_single(
        lpr3,
        diagnosis_columns=["aktionsdiagnose", "diagnosekode"],
        date_column="dato_start",
        patient_id_column="CPR",
    )

    logger.debug("Renaming CPR to PNR in LPR3 data")
    lpr3_scd = lpr3_scd.with_columns(pl.col("CPR").alias("PNR"))

    logger.info("Combining LPR2 and LPR3 SCD results")
    combined_scd = pl.concat([lpr2_scd, lpr3_scd])

    logger.info("Performing final aggregation to patient level")
    final_scd_data = combined_scd.group_by("PNR").agg(
        [
            pl.col("is_scd").max().alias("is_scd"),
            pl.col("first_scd_date").min().alias("first_scd_date"),
        ]
    )

    logger.info("Severe chronic disease identification completed")
    return final_scd_data


def process_static_data(scd_data: pl.LazyFrame) -> pl.LazyFrame:
    """Process static cohort data."""
    logger.info("Processing static cohort data")
    population = pl.scan_parquet(POPULATION_FILE)

    logger.debug("Ensuring PNR is of the same type in both dataframes")
    population = population.with_columns(pl.col("PNR").cast(pl.Utf8))
    scd_data = scd_data.with_columns(pl.col("PNR").cast(pl.Utf8))

    logger.info("Joining population data with SCD data")
    result = population.join(scd_data, left_on="PNR", right_on="PNR", how="left")

    logger.info("Static data processing completed")
    return result


def main(output_dir: Path | None = None) -> None:
    from cdef_cohort_builder.settings import settings

    logger.setLevel(settings.LOG_LEVEL.upper())  # Set log level from settings
    logger.info("Starting cohort generation process")

    if output_dir is None:
        output_dir = COHORT_FILE.parent

    logger.debug("Ensuring output directories exist")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(LPR_ADM_OUT.parent, exist_ok=True)
    os.makedirs(LPR_DIAG_OUT.parent, exist_ok=True)
    os.makedirs(LPR_BES_OUT.parent, exist_ok=True)
    os.makedirs(LPR3_DIAGNOSER_OUT.parent, exist_ok=True)
    os.makedirs(LPR3_KONTAKTER_OUT.parent, exist_ok=True)

    logger.info("Generating population data")
    generate_population()
    logger.info("Population data generation completed")

    logger.info("Identifying severe chronic diseases")
    scd_data = identify_severe_chronic_disease()
    logger.info("Severe chronic disease identification completed")

    logger.info("Processing static data")
    static_cohort = process_static_data(scd_data)
    logger.info("Static data processing completed")
    static_cohort.collect().write_parquet(STATIC_COHORT)
    logger.info(f"Static cohort data written to {STATIC_COHORT.name}")

    logger.info("Processing longitudinal data")
    combined_longitudinal_data = process_and_partition_longitudinal_data(output_dir)
    if combined_longitudinal_data is None:
        logger.error("Failed to process longitudinal data")
        return
    logger.info("Longitudinal data processing completed")

    logger.info("Identifying events")
    events = identify_events(combined_longitudinal_data, EVENT_DEFINITIONS)
    events_file = output_dir / "events.parquet"
    events.collect().write_parquet(events_file)
    logger.info("Events identified and saved")

    logger.info("Generating event summaries")
    event_summaries_dir = output_dir / "event_summaries"
    events_df = pl.read_parquet(events_file)
    generate_event_summaries(events_df, event_summaries_dir)
    logger.info("Event summaries generated")

    logger.info("Cohort generation process completed")


if __name__ == "__main__":
    main()
