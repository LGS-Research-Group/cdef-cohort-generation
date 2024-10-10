import polars as pl

IND_SCHEMA = {
    "BESKST13": pl.Int8,  # Kode for personens væsentligste indkomstkilde
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "LOENMV_13": pl.Float64,  # Lønindkomst
    "PERINDKIALT_13": pl.Float64,  # Personlig indkomst
    "PNR": pl.Utf8,
    "PRE_SOCIO": pl.Int8,  # See mapping
    "VERSION": pl.Utf8,
}

BESKST13_map = {
    1: "Selvstændig",
    2: "Medarbejdende ægtefælle",
    3: "Lønmodtager og ejer af virksomhed",
    4: "Lønmodtager",
    5: "Lønmodtager med understøttelse",
    6: "Pensionist og ejer af virksomhed",
    7: "Pensionist",
    8: "Øvrige",
    9: "Efterlønsmodtager",
    10: "Arbejdsløs mindst halvdelen af året (nettoledighed)",
    11: "Modtager af dagpenge (aktivering og lign.,sygdom, barsel og orlov)",
    12: "Kontanthjælpsmodtager",
    99: "Ikke I AKM",
}

PRE_SOCIO_map = {
    0: "Ikke i AKM",
    110: "Selvstændige",
    111: "Selvstændig, 10 eller flere ansatte",
    112: "Selvstændig, 5 - 9 ansatte",
    113: "Selvstændig, 1 - 4 ansatte",
    114: "Selvstændig, ingen ansatte",
    120: "Medarbejdende ægtefælle",
    130: "Lønmodtager",
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
