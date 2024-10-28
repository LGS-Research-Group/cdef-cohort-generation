from pathlib import Path
from typing import Any, TypedDict

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.utils.date import parse_dates

from .base import ConfigurableService
from .data_service import DataService
from .event_service import EventService
from .mapping_service import MappingService


class DiagnosisGroup(TypedDict):
    group: str
    codes: list[str]


class CohortService(ConfigurableService):
    def __init__(self, data_service: DataService, event_service: EventService, mapping_service: MappingService):
        self.data_service = data_service
        self.event_service = event_service
        self.mapping_service = mapping_service
        self._config: dict[str, Any] = {}
        self._health_data: pl.LazyFrame | None = None

    def initialize(self) -> None:
        if not self.check_valid():
            raise ValueError("Invalid configuration")

        self.data_service.initialize()
        self.event_service.initialize()
        self.mapping_service.initialize()

    def shutdown(self) -> None:
        self.data_service.shutdown()
        self.event_service.shutdown()
        self.mapping_service.shutdown()

    def configure(self, config: dict[str, Any]) -> None:
        self._config = config
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        return all(
            [self.data_service.check_valid(), self.event_service.check_valid(), self.mapping_service.check_valid()]
        )

    def process_static_data(self, scd_data: pl.LazyFrame) -> pl.LazyFrame:
        population = self.data_service.read_parquet(self._config["population_file"])
        population = population.with_columns(pl.col("PNR").cast(pl.Utf8))
        scd_data = scd_data.with_columns(pl.col("PNR").cast(pl.Utf8))
        return population.join(scd_data, on="PNR", how="left")

    def process_events(self, data: pl.LazyFrame, event_definitions: dict[str, Any], output_file: Path) -> pl.LazyFrame:
        for name, definition in event_definitions.items():
            self.event_service.register_event(name, definition)
        events = self.event_service.identify_events(data)
        self.data_service.write_parquet(events, output_file)
        return events

    def identify_severe_chronic_disease(self) -> pl.LazyFrame:
        """Process health data and identify children with severe chronic diseases."""
        # Process the health data and store it for later use
        self._health_data = self._process_health_data()

        # Get the SCD results
        scd_result = self._get_scd_results(self._health_data)

        # Return the processed results
        return scd_result

    def _apply_scd_algorithm(
        self, data: pl.LazyFrame, diagnosis_cols: list[str], date_col: str, id_col: str
    ) -> pl.LazyFrame:
        """Apply the Severe Chronic Disease (SCD) algorithm to health data.

        Args:
            data: Health data LazyFrame
            diagnosis_cols: List of column names containing diagnosis codes
            date_col: Name of the column containing dates
            id_col: Name of the column containing patient IDs

        Returns:
            LazyFrame with SCD flags and dates aggregated at patient level
        """
        # Define SCD codes
        scd_codes = [
            "D55",
            "D56",
            "D57",
            "D58",
            "D60",
            "D61",
            "D64",
            "D66",
            "D67",
            "D68",
            "D69",
            "D70",
            "D71",
            "D72",
            "D73",
            "D76",
            "D80",
            "D81",
            "D82",
            "D83",
            "D84",
            "D86",
            "D89",
            "E22",
            "E23",
            "E24",
            "E25",
            "E26",
            "E27",
            "E31",
            "E34",
            "E70",
            "E71",
            "E72",
            "E73",
            "E74",
            "E75",
            "E76",
            "E77",
            "E78",
            "E79",
            "E80",
            "E83",
            "E84",
            "E85",
            "E88",
            "F84",
            "G11",
            "G12",
            "G13",
            "G23",
            "G24",
            "G25",
            "G31",
            "G32",
            "G36",
            "G37",
            "G40",
            "G41",
            "G60",
            "G70",
            "G71",
            "G72",
            "G73",
            "G80",
            "G81",
            "G82",
            "G83",
            "G90",
            "G91",
            "G93",
            "I27",
            "I42",
            "I43",
            "I50",
            "I61",
            "I63",
            "I69",
            "I70",
            "I71",
            "I72",
            "I73",
            "I74",
            "I77",
            "I78",
            "I79",
            "J41",
            "J42",
            "J43",
            "J44",
            "J45",
            "J47",
            "J60",
            "J61",
            "J62",
            "J63",
            "J64",
            "J65",
            "J66",
            "J67",
            "J68",
            "J69",
            "J70",
            "J84",
            "J98",
            "K50",
            "K51",
            "K73",
            "K74",
            "K86",
            "K87",
            "K90",
            "M05",
            "M06",
            "M07",
            "M08",
            "M09",
            "M30",
            "M31",
            "M32",
            "M33",
            "M34",
            "M35",
            "M40",
            "M41",
            "M42",
            "M43",
            "M45",
            "M46",
            "N01",
            "N03",
            "N04",
            "N07",
            "N08",
            "N11",
            "N12",
            "N13",
            "N14",
            "N15",
            "N16",
            "N18",
            "N19",
            "N20",
            "N21",
            "N22",
            "N23",
            "N25",
            "N26",
            "N27",
            "N28",
            "N29",
            "P27",
            "Q01",
            "Q02",
            "Q03",
            "Q04",
            "Q05",
            "Q06",
            "Q07",
            "Q20",
            "Q21",
            "Q22",
            "Q23",
            "Q24",
            "Q25",
            "Q26",
            "Q27",
            "Q28",
            "Q30",
            "Q31",
            "Q32",
            "Q33",
            "Q34",
            "Q35",
            "Q36",
            "Q37",
            "Q38",
            "Q39",
            "Q40",
            "Q41",
            "Q42",
            "Q43",
            "Q44",
            "Q45",
            "Q60",
            "Q61",
            "Q62",
            "Q63",
            "Q64",
            "Q65",
            "Q66",
            "Q67",
            "Q68",
            "Q69",
            "Q70",
            "Q71",
            "Q72",
            "Q73",
            "Q74",
            "Q75",
            "Q76",
            "Q77",
            "Q78",
            "Q79",
            "Q80",
            "Q81",
            "Q82",
            "Q83",
            "Q84",
            "Q85",
            "Q86",
            "Q87",
            "Q89",
            "Q90",
            "Q91",
            "Q92",
            "Q93",
            "Q95",
            "Q96",
            "Q97",
            "Q98",
            "Q99",
        ]

        # Create SCD conditions for each diagnosis column
        scd_conditions = []
        for diag_col in diagnosis_cols:
            scd_condition = (
                pl.col(diag_col).str.to_uppercase().str.slice(1, 4).is_in(scd_codes)
                | pl.col(diag_col).str.to_uppercase().str.slice(1, 5).is_in(scd_codes)
                | (
                    (pl.col(diag_col).str.to_uppercase().str.slice(1, 4) >= pl.lit("E74"))
                    & (pl.col(diag_col).str.to_uppercase().str.slice(1, 4) <= pl.lit("E84"))
                )
                | (
                    (pl.col(diag_col).str.to_uppercase().str.slice(1, 5) >= pl.lit("P941"))
                    & (pl.col(diag_col).str.to_uppercase().str.slice(1, 5) <= pl.lit("P949"))
                )
            )
            scd_conditions.append(scd_condition)

        # Combine conditions and create result
        is_scd_expr = pl.any_horizontal(*scd_conditions)

        result = data.with_columns(
            [
                is_scd_expr.alias("is_scd"),
                pl.when(is_scd_expr).then(pl.col(date_col)).otherwise(None).alias("first_scd_date"),
            ]
        )

        # Aggregate to patient level
        return result.group_by(id_col).agg(
            [
                pl.col("is_scd").max().alias("is_scd"),
                pl.col("first_scd_date").min().alias("first_scd_date"),
            ]
        )

    def add_icd_descriptions(self, df: pl.LazyFrame, icd_file: Path) -> pl.LazyFrame:
        """Add ICD-10 descriptions to the dataframe.

        Args:
            df: Input LazyFrame containing ICD codes
            icd_file: Path to ICD descriptions file

        Returns:
            LazyFrame with added ICD-10 descriptions
        """
        icd_descriptions = pl.scan_csv(icd_file)

        return (
            df.with_columns(
                [
                    pl.col("C_ADIAG").str.slice(1).alias("icd_code_adiag"),
                    pl.col("C_DIAG").str.slice(1).alias("icd_code_diag"),
                ]
            )
            .join(
                icd_descriptions,
                left_on="icd_code_adiag",
                right_on="icd10",
                how="left",
            )
            .join(
                icd_descriptions,
                left_on="icd_code_diag",
                right_on="icd10",
                how="left",
                suffix="_diag",
            )
            .drop(["icd_code_adiag", "icd_code_diag"])
        )

    def _process_health_data(self) -> pl.LazyFrame:
        """Process and harmonize health data."""
        try:
            # Read and integrate LPR2 data
            lpr2_adm = self.data_service.read_parquet(self._config["lpr2_path"]["adm"])
            lpr2_diag = self.data_service.read_parquet(self._config["lpr2_path"]["diag"])
            lpr2_bes = self.data_service.read_parquet(self._config["lpr2_path"]["bes"])
            lpr2 = self._integrate_lpr2_components(lpr2_adm, lpr2_diag, lpr2_bes)

            # Read and integrate LPR3 data
            lpr3_kontakter = self.data_service.read_parquet(self._config["lpr3_path"]["kontakter"])
            lpr3_diagnoser = self.data_service.read_parquet(self._config["lpr3_path"]["diagnoser"])
            lpr3 = self._integrate_lpr3_components(lpr3_kontakter, lpr3_diagnoser)

            # Harmonize and combine data
            lpr2_harmonized, lpr3_harmonized = self._harmonize_health_data(lpr2, lpr3)
            combined = self._combine_harmonized_data(lpr2_harmonized, lpr3_harmonized)

            logger.info("Health data processed successfully")
            return combined

        except Exception as e:
            logger.error(f"Error processing health data: {str(e)}")
            raise ValueError(f"Error processing health data: {str(e)}") from e

    def _combine_harmonized_data(self, df1: pl.LazyFrame, df2: pl.LazyFrame) -> pl.LazyFrame:
        """Combine harmonized LPR2 and LPR3 data."""
        logger.debug("Starting combination of harmonized data")

        all_columns = set(df1.collect_schema().names()).union(set(df2.collect_schema().names()))

        for col in all_columns:
            if col not in df1.collect_schema().names():
                df1 = df1.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:
                df1 = df1.with_columns(pl.col(col).cast(pl.Utf8))

            if col not in df2.collect_schema().names():
                df2 = df2.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:
                df2 = df2.with_columns(pl.col(col).cast(pl.Utf8))

        combined_df = pl.concat([df1.select(sorted(all_columns)), df2.select(sorted(all_columns))])

        # Parse dates and ensure department handling
        combined_df = combined_df.with_columns(
            [parse_dates("admission_date"), parse_dates("discharge_date"), pl.col("department").fill_null("Unknown")]
        )

        return combined_df

    def _integrate_lpr2_components(
        self, lpr_adm: pl.LazyFrame, lpr_diag: pl.LazyFrame, lpr_bes: pl.LazyFrame
    ) -> pl.LazyFrame:
        """Integrate LPR2 components: adm, diag, and bes."""
        logger.debug("Starting LPR2 component integration")
        logger.debug(f"LPR2 ADM schema: {lpr_adm.collect_schema()}")
        logger.debug(f"LPR2 DIAG schema: {lpr_diag.collect_schema()}")
        logger.debug(f"LPR2 BES schema: {lpr_bes.collect_schema()}")

        lpr2_integrated = (
            lpr_adm.join(lpr_diag, on="RECNUM", how="left")
            .join(lpr_bes, on="RECNUM", how="left")
            .with_columns([pl.coalesce(pl.col("C_AFD"), pl.lit("Unknown")).alias("department_code")])
        )

        logger.debug(f"LPR2 final integrated schema: {lpr2_integrated.collect_schema()}")
        return lpr2_integrated

    def _integrate_lpr3_components(self, lpr3_kontakter: pl.LazyFrame, lpr3_diagnoser: pl.LazyFrame) -> pl.LazyFrame:
        """Integrate LPR3 components: kontakter and diagnoser."""
        logger.debug("Starting LPR3 component integration")
        logger.debug(f"LPR3 kontakter schema: {lpr3_kontakter.collect_schema()}")
        logger.debug(f"LPR3 diagnoser schema: {lpr3_diagnoser.collect_schema()}")

        lpr3_integrated = lpr3_kontakter.join(lpr3_diagnoser, on="DW_EK_KONTAKT", how="left").with_columns(
            [pl.coalesce(pl.col("SORENHED_ANS"), pl.lit("Unknown")).alias("department_code")]
        )

        logger.debug(f"LPR3 integrated schema: {lpr3_integrated.collect_schema()}")
        return lpr3_integrated

    def _harmonize_health_data(self, df1: pl.LazyFrame, df2: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Harmonize column names of LPR2 and LPR3 data."""
        logger.debug("Starting health data harmonization")

        column_mappings = {
            # Patient identifier
            "PNR": "patient_id",  # LPR2
            "CPR": "patient_id",  # LPR3
            # Diagnosis
            "C_ADIAG": "primary_diagnosis",  # LPR2
            "aktionsdiagnose": "primary_diagnosis",  # LPR3
            "C_DIAG": "diagnosis",  # LPR2 (from LPR_DIAG)
            "diagnosekode": "diagnosis",  # LPR3
            "C_TILDIAG": "secondary_diagnosis",  # LPR2
            # Dates
            "D_INDDTO": "admission_date",  # LPR2
            "dato_start": "admission_date",  # LPR3
            "D_UDDTO": "discharge_date",  # LPR2
            "dato_slut": "discharge_date",  # LPR3
            "D_AMBDTO": "outpatient_date",  # LPR2 (from LPR_BES)
            # Hospital and department
            "C_SGH": "hospital_code",  # LPR2
            "SORENHED_ANS": "hospital_code",  # LPR3 (assuming this is the responsible hospital)
            "C_AFD": "department_code",  # LPR2
            # Patient type and contact type
            "C_PATTYPE": "patient_type",  # LPR2
            "kontakttype": "patient_type",  # LPR3
            # Record identifier
            "RECNUM": "record_id",  # LPR2
            "DW_EK_KONTAKT": "record_id",  # LPR3
            # Additional fields
            "C_INDM": "admission_type",  # LPR2
            "prioritet": "admission_type",  # LPR3
            "C_UDM": "discharge_type",  # LPR2
            "C_SPEC": "specialty_code",  # LPR2
            "V_SENGDAGE": "bed_days",  # LPR2
            # LPR3 specific fields
            "diagnosetype": "diagnosis_type",
            "senere_afkraeftet": "diagnosis_later_disproved",
            "kontaktaarsag": "contact_reason",
            "henvisningsaarsag": "referral_reason",
            "henvisningsmaade": "referral_method",
        }

        def rename_columns(df: pl.LazyFrame) -> pl.LazyFrame:
            # Create a copy of the DataFrame with all columns
            result = df

            # First, ensure we rename department_code to department
            if "department_code" in result.collect_schema().names():
                result = result.rename({"department_code": "department"})

            # Then handle all other column renames
            for old_name, new_name in column_mappings.items():
                if old_name in result.collect_schema().names() and old_name != "department_code":
                    result = result.rename({old_name: new_name})

            return result

        # Apply renaming and add source
        df1_harmonized = rename_columns(df1).with_columns(
            [pl.lit("LPR2").alias("source"), pl.col("department").fill_null("Unknown")]
        )

        df2_harmonized = rename_columns(df2).with_columns(
            [pl.lit("LPR3").alias("source"), pl.col("department").fill_null("Unknown")]
        )

        logger.debug(f"Harmonized LPR2 schema: {df1_harmonized.collect_schema()}")
        logger.debug(f"Harmonized LPR3 schema: {df2_harmonized.collect_schema()}")

        return df1_harmonized, df2_harmonized

    def _get_scd_results(self, health_data: pl.LazyFrame) -> pl.LazyFrame:
        """Get SCD results from processed health data."""
        scd_result = self._apply_scd_algorithm(
            health_data, ["primary_diagnosis", "diagnosis", "secondary_diagnosis"], "admission_date", "patient_id"
        )

        return (
            scd_result.group_by("patient_id")
            .agg(
                [
                    pl.col("is_scd").max().alias("is_scd"),
                    pl.col("first_scd_date").min().alias("first_scd_date"),
                ]
            )
            .with_columns([pl.col("patient_id").alias("PNR")])
            .drop("patient_id")
        )

    def get_health_data_for_analytical(self) -> pl.LazyFrame | None:
        """Get processed health data for analytical dataset."""
        if self._health_data is None:
            return None

        # Process health data for analytical purposes
        return (
            self._health_data.with_columns(
                [pl.col("patient_id").alias("PNR"), pl.col("admission_date").dt.year().alias("year")]
            )
            .group_by(["PNR", "year"])
            .agg(
                [
                    pl.col("primary_diagnosis").count().alias("num_primary_diagnoses"),
                    pl.col("diagnosis").count().alias("num_diagnoses"),
                    pl.col("secondary_diagnosis").count().alias("num_secondary_diagnoses"),
                    pl.col("admission_date").count().alias("num_admissions"),
                    pl.col("discharge_date").max().alias("last_discharge"),
                    pl.col("admission_type").mode().alias("most_common_admission_type"),
                    pl.col("department").n_unique().alias("num_unique_departments"),
                ]
            )
        )

    def get_diagnosis_groups(self) -> list[DiagnosisGroup]:
        """Get the diagnosis groups used in the SCD algorithm."""
        return [
            {"group": "Blood Disorders", "codes": ["D55-D61", "D64-D73", "D76"]},
            {"group": "Immune System", "codes": ["D80-D84", "D86", "D89"]},
            {"group": "Endocrine", "codes": ["E22-E27", "E31", "E34", "E70-E85", "E88"]},
            {
                "group": "Neurological",
                "codes": ["F84", "G11-G13", "G23-G25", "G31-G32", "G36-G41", "G60-G73", "G80-G83", "G90-G91", "G93"],
            },
            {"group": "Cardiovascular", "codes": ["I27", "I42-I43", "I50", "I61", "I63", "I69-I74", "I77-I79"]},
            {"group": "Respiratory", "codes": ["J41-J45", "J47", "J60-J70", "J84", "J98"]},
            {"group": "Gastrointestinal", "codes": ["K50-K51", "K73-K74", "K86-K87", "K90"]},
            {"group": "Musculoskeletal", "codes": ["M05-M09", "M30-M35", "M40-M43", "M45-M46"]},
            {"group": "Renal", "codes": ["N01-N08", "N11-N16", "N18-N29"]},
            {"group": "Congenital", "codes": ["P27", "Q01-Q07", "Q20-Q28", "Q30-Q45", "Q60-Q99"]},
        ]

    def create_analytical_health_data(self, output_path: Path) -> None:
        """Create analytical health dataset with various views."""
        if self._health_data is None:
            raise ValueError("Health data not processed. Run identify_severe_chronic_disease first.")

        try:
            # Get base health data
            base_health = self.get_health_data_for_analytical()
            if base_health is None:
                raise ValueError("Could not get health data for analytical dataset")

            # Write longitudinal health data
            self.data_service.write_parquet(
                base_health, output_path / "longitudinal" / "health_summary.parquet", partition_by="year"
            )

            # Create and write diagnosis group summaries
            diagnosis_groups = self.get_diagnosis_groups()

            # Start with a base DataFrame containing just PNR and year
            base_summary = self._health_data.select(
                [pl.col("patient_id").alias("PNR"), pl.col("admission_date").dt.year().alias("year")]
            ).unique()

            accumulated_metrics = []

            # Calculate metrics for each diagnosis group
            for group in diagnosis_groups:
                group_name = group["group"].lower().replace(" ", "_")
                codes = group["codes"]

                # Calculate metrics for this group
                group_metrics = (
                    self._health_data.filter(
                        pl.col("primary_diagnosis").str.contains("|".join(codes))
                        | pl.col("diagnosis").str.contains("|".join(codes))
                        | pl.col("secondary_diagnosis").str.contains("|".join(codes))
                    )
                    .group_by([pl.col("patient_id").alias("PNR"), pl.col("admission_date").dt.year().alias("year")])
                    .agg(
                        [
                            pl.count().alias(f"num_{group_name}_diagnoses"),
                            pl.col("admission_date").count().alias(f"num_{group_name}_admissions"),
                        ]
                    )
                )

                accumulated_metrics.append(group_metrics)

            # Combine all metrics using a series of joins
            final_summary = base_summary
            for metrics in accumulated_metrics:
                final_summary = final_summary.join(
                    metrics.select(
                        [
                            pl.col("PNR"),
                            pl.col("year"),
                            pl.col("^num_.*$"),  # Select all columns starting with "num_"
                        ]
                    ),
                    on=["PNR", "year"],
                    how="left",
                ).with_columns(
                    [
                        pl.col("^num_.*$").fill_null(0)  # Fill nulls in all numeric columns
                    ]
                )

            # Write diagnosis group summaries
            self.data_service.write_parquet(
                final_summary, output_path / "longitudinal" / "diagnosis_groups.parquet", partition_by="year"
            )

        except Exception as e:
            logger.error(f"Error creating analytical health data: {str(e)}")
            raise ValueError(f"Error creating analytical health data: {str(e)}") from e
