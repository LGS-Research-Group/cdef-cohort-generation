from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.utils.harmonize_lpr import (
    combine_harmonized_data,
    harmonize_health_data,
    integrate_lpr2_components,
    integrate_lpr3_components,
)

from .base import ConfigurableService
from .data_service import DataService
from .event_service import EventService
from .mapping_service import MappingService


class CohortService(ConfigurableService):
    def __init__(self, data_service: DataService, event_service: EventService, mapping_service: MappingService):
        self.data_service = data_service
        self.event_service = event_service
        self.mapping_service = mapping_service
        self._config: dict[str, Any] = {}

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
            # Read and integrate LPR2 data
            lpr2_adm = self.data_service.read_parquet(self._config["lpr2_path"]["adm"])
            lpr2_diag = self.data_service.read_parquet(self._config["lpr2_path"]["diag"])
            lpr2_bes = self.data_service.read_parquet(self._config["lpr2_path"]["bes"])
            lpr2 = integrate_lpr2_components(lpr2_adm, lpr2_diag, lpr2_bes)

            # Read and integrate LPR3 data
            lpr3_kontakter = self.data_service.read_parquet(self._config["lpr3_path"]["kontakter"])
            lpr3_diagnoser = self.data_service.read_parquet(self._config["lpr3_path"]["diagnoser"])
            lpr3 = integrate_lpr3_components(lpr3_kontakter, lpr3_diagnoser)

            # Harmonize and combine data
            lpr2_harmonized, lpr3_harmonized = harmonize_health_data(lpr2, lpr3)
            combined_data = combine_harmonized_data(lpr2_harmonized, lpr3_harmonized)

            # Apply SCD algorithm to harmonized data
            scd_result = self._apply_scd_algorithm(
                combined_data,
                ["primary_diagnosis", "diagnosis", "secondary_diagnosis"],
                "admission_date",
                "patient_id"
            )

            # Return aggregated results
            return scd_result.group_by("patient_id").agg([
                pl.col("is_scd").max().alias("is_scd"),
                pl.col("first_scd_date").min().alias("first_scd_date"),
            ]).with_columns([
                pl.col("patient_id").alias("PNR")  # Rename back to PNR for consistency
            ]).drop("patient_id")

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
