import os
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs

from cdef_cohort_builder.events.plotting import (
    plot_event_heatmap,
    plot_sankey,
    plot_survival_curve,
    plot_time_series,
)
from cdef_cohort_builder.events.summaries import (
    create_interactive_dashboard,
    generate_descriptive_stats,
    generate_summary_table,
)
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


def generate_event_summaries(events_df: pl.LazyFrame, output_dir: Path) -> None:
    """Generate event summaries and plots."""
    logger.info("Generating event summaries")
    create_output_directory(output_dir)

    generate_and_save_summary_table(events_df, output_dir)
    generate_and_save_plots(events_df, output_dir)
    generate_and_save_survival_curves(events_df, output_dir)
    generate_and_save_descriptive_stats(events_df, output_dir)
    generate_and_save_interactive_dashboard(events_df, output_dir)

    logger.info(f"All visualizations and tables have been generated and saved to {output_dir}")


def create_output_directory(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def generate_and_save_summary_table(events_df: pl.LazyFrame, output_dir: Path) -> None:
    summary_table = generate_summary_table(events_df)
    summary_table.write_csv(output_dir / "summary_table.csv")


def generate_and_save_plots(events_df: pl.LazyFrame, output_dir: Path) -> None:
    matplotlib_plots = [
        ("time_series_plot", plot_time_series),
        ("event_heatmap", plot_event_heatmap),
    ]

    for plot_name, plot_function in matplotlib_plots:
        fig = plot_function(events_df)
        fig.savefig(output_dir / f"{plot_name}.png")
        plt.close(fig)

    # Generate and save Sankey diagram
    event_sequence = get_event_sequence(events_df)
    sankey = plot_sankey(events_df, event_sequence)
    sankey.write_html(output_dir / "sankey_diagram.html")


def get_event_sequence(events_df: pl.LazyFrame) -> list[str]:
    return events_df.select(pl.col("event_type").unique()).collect().to_series().to_list()


def generate_and_save_survival_curves(events_df: pl.LazyFrame, output_dir: Path) -> None:
    event_types = get_event_sequence(events_df)

    for event_type in event_types:
        survival_curve = plot_survival_curve(events_df, event_type)
        if survival_curve is not None:
            survival_curve.savefig(output_dir / f"survival_curve_{event_type}.png")
            plt.close(survival_curve)
        else:
            logger.warning(f"Skipping survival curve for {event_type} due to insufficient data")


def generate_and_save_descriptive_stats(events_df: pl.LazyFrame, output_dir: Path) -> None:
    numeric_cols = cs.expand_selector(events_df, cs.numeric())
    if numeric_cols:
        desc_stats = generate_descriptive_stats(events_df, list(numeric_cols))
        desc_stats.write_csv(output_dir / "descriptive_stats.csv")
    else:
        logger.warning("No numeric columns found for descriptive statistics")


def generate_and_save_interactive_dashboard(events_df: pl.LazyFrame, output_dir: Path) -> None:
    dashboard = create_interactive_dashboard(events_df)
    dashboard.write_html(output_dir / "interactive_dashboard.html")


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
    logger.info("Longitudinal data processing completed")

    logger.info("Identifying events")
    events = identify_events(combined_longitudinal_data, EVENT_DEFINITIONS)
    events_file = output_dir / "events.parquet"
    events.collect().write_parquet(events_file)
    logger.info("Events identified and saved")

    logger.info("Generating event summaries")
    event_summaries_dir = output_dir / "event_summaries"
    generate_event_summaries(events, event_summaries_dir)
    logger.info("Event summaries generated")

    logger.info("Cohort generation process completed")


if __name__ == "__main__":
    main()
