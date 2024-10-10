import polars as pl

from cdef_cohort_generation.config import AKM_FILES, AKM_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

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


def process_akm() -> None:
    process_register_data(
        input_files=AKM_FILES,
        output_file=AKM_OUT,
        population_file=POPULATION_FILE,
        schema=AKM_SCHEMA,
        columns_to_keep=[
            "PNR",
            "SOCIO13",
            "SENR",
        ],
    )


if __name__ == "__main__":
    process_akm()
