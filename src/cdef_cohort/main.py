import logging
import os
from pathlib import Path
from typing import Any

import polars as pl
import polars.selectors as cs
from cdef_cohort.events.plotting import save_plot_data
from cdef_cohort.events.summaries import (
    generate_descriptive_stats,
    generate_summary_table,
)
from cdef_cohort.functions.population_summary import (
    save_event_summary,
    save_scd_summary,
)
from cdef_cohort.logging_config import logger
from cdef_cohort.population import main as generate_population
from cdef_cohort.registers import (
    longitudinal,
    lpr3_diagnoser,
    lpr3_kontakter,
    lpr_adm,
    lpr_bes,
    lpr_diag,
)
from cdef_cohort.utils.config import (
    CHILD_EVENT_DEFINITIONS,
    COHORT_FILE,
    FATHER_EVENT_DEFINITIONS,
    LPR3_DIAGNOSER_OUT,
    LPR3_KONTAKTER_OUT,
    LPR_ADM_OUT,
    LPR_BES_OUT,
    LPR_DIAG_OUT,
    MOTHER_EVENT_DEFINITIONS,
    POPULATION_FILE,
    STATIC_COHORT,
)
from cdef_cohort.utils.event import identify_events
from cdef_cohort.utils.harmonize_lpr import (
    integrate_lpr2_components,
    integrate_lpr3_components,
)
from cdef_cohort.utils.hash_utils import process_with_hash_check
from cdef_cohort.utils.icd import apply_scd_algorithm_single

logging.getLogger("polars").setLevel(logging.WARNING)


def process_lpr_data() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Process LPR2 and LPR3 data."""
    logger.info("Processing LPR data")

    # Process LPR2 data
    process_with_hash_check(
        lpr_adm.process_lpr_adm, columns_to_keep=["PNR", "C_ADIAG", "RECNUM", "D_INDDTO"]
    )
    process_with_hash_check(
        lpr_diag.process_lpr_diag, columns_to_keep=["RECNUM", "C_DIAG", "C_TILDIAG"]
    )
    process_with_hash_check(lpr_bes.process_lpr_bes, columns_to_keep=["D_AMBDTO", "RECNUM"])

    # Process LPR3 data
    process_with_hash_check(
        lpr3_diagnoser.process_lpr3_diagnoser, columns_to_keep=["DW_EK_KONTAKT", "diagnosekode"]
    )
    process_with_hash_check(
        lpr3_kontakter.process_lpr3_kontakter,
        columns_to_keep=["DW_EK_KONTAKT", "CPR", "aktionsdiagnose", "dato_start"],
    )

    # Integrate components
    lpr2 = integrate_lpr2_components(
        pl.scan_parquet(LPR_ADM_OUT), pl.scan_parquet(LPR_DIAG_OUT), pl.scan_parquet(LPR_BES_OUT)
    )
    lpr3 = integrate_lpr3_components(
        pl.scan_parquet(LPR3_KONTAKTER_OUT), pl.scan_parquet(LPR3_DIAGNOSER_OUT)
    )

    return lpr2, lpr3


def apply_scd_algorithm(
    lpr_data: pl.LazyFrame, diagnosis_cols: list[str], date_col: str, id_col: str
) -> pl.LazyFrame:
    """Apply SCD algorithm to LPR data."""
    return apply_scd_algorithm_single(
        lpr_data,
        diagnosis_columns=diagnosis_cols,
        date_column=date_col,
        patient_id_column=id_col,
    )


def identify_severe_chronic_disease() -> pl.LazyFrame:
    """Process health data and identify children with severe chronic diseases."""
    logger.info("Starting identification of severe chronic diseases")

    lpr2, lpr3 = process_lpr_data()

    lpr2_scd = apply_scd_algorithm(lpr2, ["C_ADIAG", "C_DIAG", "C_TILDIAG"], "D_INDDTO", "PNR")
    lpr3_scd = apply_scd_algorithm(lpr3, ["aktionsdiagnose", "diagnosekode"], "dato_start", "CPR")

    lpr3_scd = lpr3_scd.with_columns(pl.col("CPR").alias("PNR"))

    combined_scd = pl.concat([lpr2_scd, lpr3_scd])

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

    population = population.with_columns(pl.col("PNR").cast(pl.Utf8))
    scd_data = scd_data.with_columns(pl.col("PNR").cast(pl.Utf8))

    result = population.join(scd_data, on="PNR", how="left")

    logger.info("Static data processing completed")
    return result


def generate_event_summaries(events_df: pl.LazyFrame, output_dir: Path) -> None:
    """Generate event summaries and save plot data."""
    logger.info("Generating event summaries")
    output_dir.mkdir(parents=True, exist_ok=True)

    if events_df.collect().is_empty():
        logger.warning("No events identified. Skipping summary generation.")
        return

    # Generate and save summary table
    summary_table = generate_summary_table(events_df)
    summary_table.write_csv(output_dir / "summary_table.csv")

    # Generate and save descriptive statistics
    numeric_cols = cs.expand_selector(events_df, cs.numeric())
    if numeric_cols:
        desc_stats = generate_descriptive_stats(events_df, list(numeric_cols))
        desc_stats.write_csv(output_dir / "descriptive_stats.csv")
    else:
        logger.warning("No numeric columns found for descriptive statistics")

    # Save plot data
    save_plot_data(events_df, output_dir / "plot_data")

    logger.info(f"All summaries and plot data have been generated and saved to {output_dir}")


def process_events(
    data: pl.LazyFrame, event_definitions: dict[str, Any], output_file: Path, event_type: str
) -> pl.LazyFrame:
    """Process events for a given dataset."""
    logger.debug(f"Schema before event identification: {data.collect_schema()}")
    events = identify_events(data, event_definitions, event_type)
    events.collect().write_parquet(output_file)
    return events


def main(output_dir: Path | None = None) -> None:
    from cdef_cohort.settings import settings

    logger.setLevel(str(settings.LOG_LEVEL.upper()))
    logger.info("Starting cohort generation process")

    output_dir = output_dir or COHORT_FILE.parent
    os.makedirs(output_dir, exist_ok=True)

    # Generate population data
    generate_population()

    # Process severe chronic disease data
    scd_data = identify_severe_chronic_disease()
    save_scd_summary(scd_data, output_dir / "scd_summary.csv")

    # Process static data
    static_cohort = process_static_data(scd_data)
    static_cohort.collect().write_parquet(STATIC_COHORT)

    # Process longitudinal data
    child_data, mother_data, father_data = longitudinal.process_and_partition_longitudinal_data(
        output_dir
    )

    # Process events
    events = []
    events.append(
        process_events(
            child_data, CHILD_EVENT_DEFINITIONS, output_dir / "child_events.parquet", "child"
        )
    )

    if father_data is not None:
        events.append(
            process_events(
                father_data,
                FATHER_EVENT_DEFINITIONS,
                output_dir / "father_events.parquet",
                "father",
            )
        )
    else:
        logger.warning("No father data available for event identification")

    if mother_data is not None:
        events.append(
            process_events(
                mother_data,
                MOTHER_EVENT_DEFINITIONS,
                output_dir / "mother_events.parquet",
                "mother",
            )
        )
    else:
        logger.warning("No mother data available for event identification")

    # Generate event summaries
    all_events = pl.concat(events)
    generate_event_summaries(all_events, output_dir / "event_summaries")
    save_event_summary(all_events, output_dir / "event_summary.csv")

    logger.info("Cohort generation process completed")


if __name__ == "__main__":
    main()
