from pathlib import Path

import polars as pl

from cdef_cohort_generation.config import (
    AKM_OUT,
    BEF_OUT,
    COHORT_FILE,
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
from cdef_cohort_generation.registers.akm import process_akm
from cdef_cohort_generation.registers.bef import process_bef
from cdef_cohort_generation.registers.idan import process_idan
from cdef_cohort_generation.registers.ind import process_ind
from cdef_cohort_generation.registers.lpr3_diagnoser import process_lpr3_diagnoser
from cdef_cohort_generation.registers.lpr3_kontakter import process_lpr3_kontakter
from cdef_cohort_generation.registers.lpr_adm import process_lpr_adm
from cdef_cohort_generation.registers.lpr_bes import process_lpr_bes
from cdef_cohort_generation.registers.lpr_diag import process_lpr_diag
from cdef_cohort_generation.registers.uddf import process_uddf
from cdef_cohort_generation.utils import apply_scd_algorithm


def identify_severe_chronic_disease() -> pl.DataFrame:
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
    lpr_adm = pl.read_parquet(LPR_ADM_OUT)
    lpr_diag = pl.read_parquet(LPR_DIAG_OUT)
    lpr_bes = pl.read_parquet(LPR_BES_OUT)
    lpr3_diagnoser = pl.read_parquet(LPR3_DIAGNOSER_OUT)
    lpr3_kontakter = pl.read_parquet(LPR3_KONTAKTER_OUT)

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


def process_cohort_data(scd_data: pl.DataFrame, output_file: Path) -> None:
    """
    Process cohort data by joining SCD information with population and other register data.

    Args:
    scd_data (pl.DataFrame): DataFrame with SCD classification.
    output_file (Path): Path to save the output file.
    """

    # Step 1: Read population data
    population = pl.read_parquet(POPULATION_FILE)

    # Step 2: Join SCD data with population
    cohort = population.join(scd_data, on="PNR", how="left")

    # Step 3: Process and join other register data
    process_bef()
    process_uddf()
    process_akm()
    process_ind()
    process_idan()

    other_registers = [BEF_OUT, UDDF_OUT, AKM_OUT, IND_OUT, IDAN_OUT]
    for register in other_registers:
        register_data = pl.read_parquet(register)
        cohort = cohort.join(register_data, on="PNR", how="left")

    # Step 4: Add additional information
    cohort = cohort.with_columns(
        [
            pl.col("first_scd_date").dt.offset_by("-1y").alias("pre_exposure_start"),
            pl.col("first_scd_date").alias("pre_exposure_end"),
        ]
    )

    # Step 5: Write output to file
    cohort.write_parquet(output_file)
    print(f"Cohort data written to {output_file}")


# Main execution
def main(output_file: Path) -> None:
    scd_data = identify_severe_chronic_disease()
    process_cohort_data(scd_data, output_file)


if __name__ == "__main__":
    main(COHORT_FILE)
