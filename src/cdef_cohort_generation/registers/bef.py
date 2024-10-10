import polars as pl

from cdef_cohort_generation.config import BEF_FILES, BEF_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import parse_dates

BEF_SCHEMA = {
    "AEGTE_ID": pl.Utf8,
    "ALDER": pl.Int8,
    "ANTBOERNF": pl.Int8,
    "ANTBOERNH": pl.Int8,
    "ANTPERSF": pl.Int8,
    "ANTPERSH": pl.Int8,
    "BOP_VFRA": pl.Date,
    "CIVST": pl.Utf8,
    "CPRTJEK": pl.Int8,
    "CPRTYPE": pl.Int8,
    "E_FAELLE_ID": pl.Utf8,
    "FAMILIE_ID": pl.Utf8,
    "FAMILIE_TYPE": pl.UInt8,  # ved ikke hvordan den her varible ser ud
    "FAR_ID": pl.Utf8,
    "FM_MARK": pl.Int8,
    "FOED_DAG": pl.Date,
    "HUSTYPE": pl.Int8,
    "IE_TYPE": pl.Utf8,  # ved ikke hvordan den her varible ser ud
    "KOEN": pl.Utf8,  # ved ikke hvordan den her varible ser ud
    "KOM": pl.Int8,  # 2-3 cifret kode
    "MOR_ID": pl.Utf8,
    "OPR_LAND": pl.Utf8,  # ved ikke hvordan den har varitable ser ud
    "PLADS": pl.Int8,
    "PNR": pl.Utf8,
    "REG": pl.Int8,
    "STATSB": pl.Int8,
    "VERSION": pl.Utf8,
}

REG = {  # Region
    0: "Uoplyst",
    81: "Nordjylland",
    82: "Midtjylland",
    83: "Syddanmark",
    84: "Hovedstaden",
    85: "Sjælland",
}

PLADS = {  # Plads
    1: "Hovedperson",
    2: "Ægtefælle/partner",
    3: "Hjemmeboende barn",
}

HUSTYPE = {  # Husstandstype
    1: "En enlig mand",
    2: "En enlig kvinde",
    3: "Et ægtepar",
    4: "Et par i øvrigt",
    5: "Et ikke-hjemmeboende barn under 18 år",
    6: "Husstand bestående af flere familier",
}

FM_MARK = {  # Familie markering
    1: "Bor sammen med begge forældrene",
    2: "For børn: Bor hos mor, der er i nyt par. For voksne: Bor sammen med mor.",
    3: "For børn: Bor hos enlig mor. For voksneVværdien findes ikke.",
    4: "For børn: Bor hos far, der er i nyt par. For voksne: Bor sammen med far.",
    5: "For børn: Bor hos enlig far. For voksne: Værdien findes ikke.",
    6: "Bor ikke hos forældrene",
}

CIVST = {  # Civilstand
    "D": "Død",
    "E": "Enke/Enkemand",
    "F": "Skilt",
    "G": "Gift (+ separeret)",
    "L": "Længstlevende af 2 partnere",
    "O": "Ophævet partnerskab",
    "P": "Registreret partnerskab",
    "U": "Ugift",
    "9": "Uoplyst civilstand",
}


def main() -> None:
    # Read all bef parquet files
    bef_files = BEF_FILES
    bef = pl.scan_parquet(bef_files, allow_missing_columns=True).with_columns(
        [parse_dates("FOED_DAG").alias("FOED_DAG"), parse_dates("BOP_VFRA").alias("BOP_VFRA")]
    )

    # Discard columns we do not needed
    bef = bef.select(
        [
            "AEGTE_ID",
            "ALDER",
            "ANTBOERNF",
            "ANTBOERNH",
            "ANTPERSF",
            "ANTPERSH",
            "BOP_VFRA",
            "CIVST",
            "E_FAELLE_ID",
            "FAMILIE_ID",
            "FAMILIE_TYPE",  # ved ikke hvordan den her varible ser ud
            "FAR_ID",
            "FM_MARK",
            "FOED_DAG",
            "HUSTYPE",
            "IE_TYPE",  # ved ikke hvordan den her varible ser ud
            "KOEN",  # ved ikke hvordan den her varible ser ud
            "KOM",  # 2-3 cifret kode
            "MOR_ID",
            "OPR_LAND",  # ved ikke hvordan den har varitable ser ud
            "PLADS",
            "PNR",
            "REG",
            "STATSB",
        ]
    )

    # Read in population file to merge with
    population = pl.read_parquet(POPULATION_FILE)

    # Write result into parquet file
    population.write_parquet(BEF_OUT)


if __name__ == "__main__":
    from typing import TYPE_CHECKING

    if not TYPE_CHECKING:
        main()
