from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.models.config import PipelineConfig, PipelineStageConfig
from cdef_cohort.services.analytical_data_service import AnalyticalDataService
from cdef_cohort.services.base import ConfigurableService
from cdef_cohort.services.cohort_service import CohortService
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.population_service import PopulationService
from cdef_cohort.services.register_service import RegisterService


class PipelineService(ConfigurableService):
    def __init__(
        self,
        register_service: RegisterService,
        cohort_service: CohortService,
        population_service: PopulationService,
        data_service: DataService,
        analytical_data_service: AnalyticalDataService,
    ):
        self.register_service = register_service
        self.cohort_service = cohort_service
        self.population_service = population_service
        self.data_service = data_service
        self.analytical_data_service = analytical_data_service
        self._config: PipelineConfig | None = None
        self._stage_results: dict[str, pl.LazyFrame] = {}

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._stage_results.clear()

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return self._config is not None

    def configure(self, config: dict[str, Any]) -> None:
        """Configure pipeline with stages and dependencies"""
        pipeline_config = PipelineConfig(**config)
        self._config = pipeline_config

        # Configure analytical data service
        self.analytical_data_service.configure(
            {
                "output_base_path": pipeline_config.output_configs.analytical_data.base_path,
                "stage_results": self._stage_results,
                "population_file": pipeline_config.stage_configs["population"].output_file,
            }
        )

    def get_final_result(self, results: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        """Get the final result from the pipeline results."""
        if "final_cohort" in results:
            return results["final_cohort"]

        if not self._config:
            raise ValueError("Pipeline configuration is not set")

        final_stage = self._config.stage_order[-1]
        if final_stage not in results:
            raise ValueError(f"Final stage {final_stage} not found in results")

        return results[final_stage]

    def run_pipeline(self) -> dict[str, pl.LazyFrame]:
        """Run the complete data pipeline"""
        if not self._config:
            raise ValueError("Pipeline not configured")

        logger.info("Starting pipeline execution")

        try:
            # Execute stages in order
            for stage_name in self._config.stage_order:
                self._execute_stage(stage_name)

            # Set the final result
            if self._stage_results:
                final_stage = self._config.stage_order[-1]
                self._stage_results["final_cohort"] = self._stage_results[final_stage]

            # Configure analytical data service
            self.analytical_data_service.configure(
                {
                    "output_base_path": self._config.output_configs.analytical_data.base_path,
                    "stage_results": self._stage_results,
                    "population_file": self._config.stage_configs["population"].output_file,
                }
            )
            self.analytical_data_service.create_analytical_dataset()

            logger.info("Pipeline execution completed successfully")
            return self._stage_results

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

    def _execute_stage(self, stage_name: str) -> None:
        """Execute a single pipeline stage"""
        if not self._config:
            raise ValueError("Pipeline not configured")

        stage_config = self._config.stage_configs[stage_name]
        logger.info(f"Executing pipeline stage: {stage_name}")

        # Check dependencies
        self._check_dependencies(stage_config)

        # Execute stage based on type
        result = self._get_stage_result(stage_name, stage_config)

        if result is None:
            raise ValueError(f"Stage {stage_name} produced no result")

        # Save result (now we know result is not None)
        self._stage_results[stage_name] = result

        # Write output if configured
        if stage_config.output_file:
            output_path = Path(stage_config.output_file)
            self.data_service.write_parquet(result, output_path)

    def _get_stage_result(self, stage_name: str, stage_config: PipelineStageConfig) -> pl.LazyFrame:
        """Get result for a specific stage"""
        if stage_config.register_name:
            return self.register_service.process_register_data(stage_config.register_name)
        elif stage_name == "population":
            result = self.population_service.process_population()
            if not isinstance(result, pl.LazyFrame):
                raise ValueError(f"Population service returned {type(result)}, expected LazyFrame")
            return result
        elif stage_name == "health":
            return self.cohort_service.identify_severe_chronic_disease()
        else:
            return self._execute_custom_stage(stage_config)

    def _check_dependencies(self, stage_config: PipelineStageConfig) -> None:
        """Check if all stage dependencies are satisfied"""
        for dep in stage_config.depends_on:
            if dep not in self._stage_results:
                raise ValueError(f"Dependency not satisfied: {dep}")

    def _execute_custom_stage(self, stage_config: PipelineStageConfig) -> pl.LazyFrame:
        """Execute a custom pipeline stage"""
        # Implement custom stage logic here
        raise NotImplementedError(f"Custom stage not implemented: {stage_config.name}")
