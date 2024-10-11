from pathlib import Path

import polars as pl

from cdef_cohort_generation.config import (
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
)
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
from cdef_cohort_generation.utils import apply_scd_algorithm, identify_events


def identify_severe_chronic_disease() -> pl.LazyFrame:
    """
    Process health data and identify children with severe chronic diseases.

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

    # Step 5: Combine all health data
    health_data = pl.concat([lpr2, lpr3])

    # Step 6: Apply SCD algorithm
    scd_data = apply_scd_algorithm(health_data)

    # Step 7: Aggregate to patient level
    return scd_data.group_by("PNR").agg(
        [
            pl.col("is_scd").max().alias("is_scd"),
            pl.col("first_scd_date").min().alias("first_scd_date"),
        ]
    )


def process_static_data(scd_data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Process static cohort data.
    """
    population = pl.scan_parquet(POPULATION_FILE)
    static_cohort = population.join(scd_data, on="PNR", how="left")

    # Add any other static data processing here

    return static_cohort


def process_longitudinal_data() -> pl.LazyFrame:
    """
    Process longitudinal data from various registers.
    """
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
def main(output_dir: Path) -> None:
    # Process health data and identify SCD
    scd_data = identify_severe_chronic_disease()

    # Process static data
    static_cohort = process_static_data(scd_data)
    static_cohort.collect().write_parquet(output_dir / "static_cohort.parquet")
    print(f"Static cohort data written to {output_dir / 'static_cohort.parquet'}")

    # Process longitudinal data
    longitudinal_data = process_longitudinal_data()
    longitudinal_data.collect().write_parquet(output_dir / "longitudinal_data.parquet")
    print(f"Longitudinal data written to {output_dir / 'longitudinal_data.parquet'}")

    # Identify events
    events = identify_events(longitudinal_data, EVENT_DEFINITIONS)
    events.collect().write_parquet(output_dir / "events.parquet")
    print(f"Events data written to {output_dir / 'events.parquet'}")


if __name__ == "__main__":
    main(COHORT_FILE.parent)
