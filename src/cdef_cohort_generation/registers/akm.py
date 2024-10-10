import polars as pl

from cdef_cohort_generation.config import AKM_FILES, AKM_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import parse_dates

AKM_SCHEMA = {
    "PNR": pl.Utf8,
    "SOCIO": pl.Int8,
    "SOCIO02": pl.Int8,
    "SOCIO13": pl.Int8,  # <- Only one we are interested in
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "VERSION": pl.Utf8,
    "SENR": pl.Utf8,  # Dont know the structure of this
}

SOCIO13_map = {
    0: "Ikke i AKM",
    110: "Selvstændig",
    111: "Selvstændig, 10 eller flere ansatte",
    112: "Selvstændig, 5 - 9 ansatte",
    113: "Selvstændig, 1 - 4 ansatte",
    114: "Selvstændig, ingen ansatte",
    120: "Medarbejdende ægtefælle",
    131: "Lønmodtager med ledelsesarbejde",
    132: "Lønmodtager i arbejde der forudsætter færdigheder på højeste niveau",
    133: "Lønmodtager i arbejde der forudsætter færdigheder på mellemniveau",
    134: "Lønmodtager i arbejde der forudsætter færdigheder på grundniveau",
    135: "Andre lønmodtagere",
    139: "Lønmodtager, stillingsangivelse ikke oplyst",
    210: "Arbejdsløs mindst halvdelen af året",
    220: "Modtager af sygedagpenge, uddannelsesgodtgørelse, orlovsydelser mm.",
    310: "Under uddannelse, inkl.skoleelever min. 15 år",
    321: "Førtidspensionister",
    322: "Folkepensionister",
    323: "Efterlønsmodtager mv.",
    330: "Kontanthjælpsmodtager",
    410: "Andre",
    420: "Børn under 15 år ultimo året",
}


def main() -> None:
    # Read all bef parquet files
    akm_files = AKM_FILES
    akm = pl.scan_parquet(akm_files, allow_missing_columns=True).with_columns(
        [parse_dates("FOED_DAG"), parse_dates("BOP_VFRA")]
    )

    # Read in the population file and join with the akm data
    population = pl.scan_parquet(POPULATION_FILE).join(akm, on="PNR", how="left").collect()

    # Save the data
    population.write_parquet(AKM_OUT)
