import polars as pl

BEF_SCHEMA = {
    "AEGTE_ID": pl.Utf8,  # PNR of the spouse
    "ALDER": pl.Int8,  # Age (derived from FOED_DAG)
    "ANTBOERNF": pl.Int8,  # Number of children in the family
    "ANTBOERNH": pl.Int8,  # Number of children in the household
    "ANTPERSF": pl.Int8,  # Number of persons in the family
    "ANTPERSH": pl.Int8,  # Number of persons in the household
    "BOP_VFRA": pl.Date,  # Date of moving in
    "CIVST": pl.Categorical,  # Civil status
    "CPRTJEK": pl.Int8,  # Checksum for CPR/PNR number
    "CPRTYPE": pl.Int8,  # Type of CPR/PNR number
    "E_FAELLE_ID": pl.Utf8,  # PNR of the partner
    "FAMILIE_ID": pl.Utf8,  # Unique family ID
    "FAMILIE_TYPE": pl.Utf8,  # Family type
    "FAR_ID": pl.Utf8,  # PNR of the father
    "FM_MARK": pl.Categorical,  # Family mark
    "FOED_DAG": pl.Date,  # Date of birth
    "HUSTYPE": pl.Categorical,  # Household type
    "IE_TYPE": pl.Utf8,  # Immigration/emigration type
    "KOEN": pl.Utf8,  # Gender
    "KOM": pl.Int16,  # Municipality code
    "MOR_ID": pl.Utf8,  # PNR of the mother
    "OPR_LAND": pl.Utf8,  # Country of origin
    "PLADS": pl.Categorical,  # The person's place in the family
    "PNR": pl.Utf8,  # CPR/PNR number
    "REG": pl.Categorical,  # Region
    "STATSB": pl.Categorical,  # Citizenship
    "VERSION": pl.Utf8,  # Version of the data
}

BEF_REQUIRED_COLUMNS = {"PNR", "FOED_DAG", "FAR_ID", "MOR_ID", "FAMILIE_ID", "KOM"}


AKM_SCHEMA = {
    "PNR": pl.Utf8,
    "SOCIO": pl.Int8,
    "SOCIO02": pl.Int8,
    "SOCIO13": pl.Categorical,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "VERSION": pl.Utf8,
    "SENR": pl.Utf8,
}

IND_SCHEMA = {
    "BESKST13": pl.Int8,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "LOENMV_13": pl.Float64,
    "PERINDKIALT_13": pl.Float64,
    "PNR": pl.Utf8,
    "PRE_SOCIO": pl.Int8,
    "VERSION": pl.Utf8,
}


LPR3_DIAGNOSER_SCHEMA = {
    "DW_EK_KONTAKT": pl.Utf8,
    "diagnosekode": pl.Utf8,
    "diagnosetype": pl.Utf8,
    "senere_afkraeftet": pl.Utf8,
    "diagnosekode_parent": pl.Utf8,
    "diagnosetype_parent": pl.Utf8,
    "lprindberetningssystem": pl.Utf8,
}


LPR3_KONTAKTER_SCHEMA = {
    "SORENHED_IND": pl.Utf8,
    "SORENHED_HEN": pl.Utf8,
    "SORENHED_ANS": pl.Utf8,
    "DW_EK_KONTAKT": pl.Utf8,
    "DW_EK_FORLOEB": pl.Utf8,
    "CPR": pl.Utf8,
    "dato_start": pl.Utf8,
    "tidspunkt_start": pl.Utf8,
    "dato_slut": pl.Utf8,
    "tidspunkt_slut": pl.Utf8,
    "aktionsdiagnose": pl.Utf8,
    "kontaktaarsag": pl.Utf8,
    "prioritet": pl.Utf8,
    "kontakttype": pl.Utf8,
    "henvisningsaarsag": pl.Utf8,
    "henvisningsmaade": pl.Utf8,
    "dato_behandling_start": pl.Utf8,
    "tidspunkt_behandling_start": pl.Utf8,
    "dato_indberetning": pl.Utf8,
    "lprindberetningssytem": pl.Utf8,
}


LPR_ADM_SCHEMA = {
    "PNR": pl.Utf8,  # Personnummer
    "C_ADIAG": pl.Utf8,  # Aktionsdiagnose
    "C_AFD": pl.Utf8,  # Afdelingskode
    "C_HAFD": pl.Utf8,  # Henvisende afdeling
    "C_HENM": pl.Utf8,  # Henvisningsmåde
    "C_HSGH": pl.Utf8,  # Henvisende sygehus
    "C_INDM": pl.Utf8,  # Indlæggelsesmåde
    "C_KOM": pl.Utf8,  # Kommune
    "C_KONTAARS": pl.Utf8,  # Kontaktårsag
    "C_PATTYPE": pl.Utf8,  # Patienttype
    "C_SGH": pl.Utf8,  # Sygehus
    "C_SPEC": pl.Utf8,  # Specialekode
    "C_UDM": pl.Utf8,  # Udskrivningsmåde
    "CPRTJEK": pl.Utf8,  # CPR-tjek
    "CPRTYPE": pl.Utf8,  # CPR-type
    "D_HENDTO": pl.Date,  # Henvisningsdato
    "D_INDDTO": pl.Date,  # Indlæggelsesdato
    "D_UDDTO": pl.Date,  # Udskrivningsdato
    "K_AFD": pl.Utf8,  # Afdelingskode
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "V_ALDDG": pl.Int32,  # Alder i dage ved kontaktens start
    "V_ALDER": pl.Int32,  # Alder i år ved kontaktens start
    "V_INDMINUT": pl.Int32,  # Indlæggelsminut
    "V_INDTIME": pl.Int32,  # Indlæggelsestidspunkt
    "V_SENGDAGE": pl.Int32,  # Sengedage
    "V_UDTIME": pl.Int32,  # Udskrivningstime
    "VERSION": pl.Utf8,  # DST Version
}


LPR_BES_SCHEMA = {
    "D_AMBDTO": pl.Date,  # Dato for ambulantbesøg
    "LEVERANCEDATO": pl.Date,  # DST leverancedato
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "VERSION": pl.Utf8,  # DST Version
}


LPR_DIAG_SCHEMA = {
    "C_DIAG": pl.Utf8,  # Diagnosekode
    "C_DIAGTYPE": pl.Utf8,  # Diagnosetype
    "C_TILDIAG": pl.Utf8,  # Tillægsdiagnose
    "LEVERANCEDATO": pl.Date,  # DST leverancedato
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "VERSION": pl.Utf8,  # DST Version
}


UDDF_SCHEMA = {
    "PNR": pl.Utf8,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "HFAUDD": pl.Utf8,
    "HF_KILDE": pl.Utf8,
    "HF_VFRA": pl.Utf8,
    "HF_VTIL": pl.Utf8,
    "INSTNR": pl.Int8,
    "VERSION": pl.Utf8,
}
