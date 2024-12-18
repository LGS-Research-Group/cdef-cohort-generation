import os
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger, validate_log_level
from cdef_cohort.schemas.all import (
    AKM_SCHEMA,
    BEF_SCHEMA,
    IND_SCHEMA,
    UDDF_SCHEMA,
)
from cdef_cohort.services.container import get_container
from cdef_cohort.settings import settings


def ensure_population_file_exists() -> None:
    """Ensure population file exists by creating it if necessary."""
    from cdef_cohort.population import main as population_main

    if not Path(settings.POPULATION_FILE).exists():
        logger.info("Population file not found - creating it...")
        population_main()
        if not Path(settings.POPULATION_FILE).exists():
            raise ValueError("Failed to create population file")
        logger.info("Population file created successfully")


def create_register_configs() -> dict:
    """Create register configurations with proper schema conversions"""
    return {
        "bef": {
            "name": "bef",
            "input_files": str(settings.BEF_FILES),
            "output_file": str(settings.BEF_OUT),
            "schema_def": BEF_SCHEMA,
            "defaults": {
                "longitudinal": True,
                "temporal_key": "year",
                "columns_to_drop": [
                    "VERSION",
                    "CPRTYPE",
                    "CPRTJEK",
                ],
            },
        },
        "akm": {
            "name": "akm",
            "input_files": str(settings.AKM_FILES),
            "output_file": str(settings.AKM_OUT),
            "schema_def": AKM_SCHEMA,
            "defaults": {
                "longitudinal": True,
                "temporal_key": "year",
                "columns_to_drop": [
                    "SOCIO",
                    "SOCIO02",
                    "CPRTJEK",
                    "CPRTYPE",
                    "VERSION",
                ],
            },
        },
        "ind": {
            "name": "ind",
            "input_files": str(settings.IND_FILES),
            "output_file": str(settings.IND_OUT),
            "schema_def": IND_SCHEMA,
            "defaults": {
                "longitudinal": True,
                "temporal_key": "year",
                "columns_to_drop": [
                    "CPRTJEK",
                    "CPRTYPE",
                    "VERSION",
                ],
            },
        },
        "uddf": {
            "name": "uddf",
            "input_files": str(settings.UDDF_FILES),
            "output_file": str(settings.UDDF_OUT),
            "schema_def": UDDF_SCHEMA,
            "defaults": {
                "longitudinal": True,
                "temporal_key": "year",
                "columns_to_drop": [
                    "CPRTJEK",
                    "CPRTYPE",
                    "VERSION",
                ],
                "apply_mappings": ["isced"],
            },
        },
    }


def configure_statistics(config: dict[str, Any]) -> dict[str, Any]:
    """Configure statistics calculation"""
    output_dir = Path(config["output_dir"])
    return {
        "output_path": output_dir / "statistics",
        "domains": {
            "demographics": {
                "numeric": ["household_size", "age"],
                "categorical": ["sex", "municipality", "region"],
                "temporal": ["birth_date"],
            },
            "education": {
                "numeric": ["education_level"],
                "categorical": ["education_field"],
            },
            "income": {
                "numeric": ["annual_income", "disposable_income"],
            },
            "employment": {
                "categorical": ["employment_status", "sector"],
            },
        },
    }


def main(output_dir: Path | None = None) -> pl.LazyFrame:
    # Validate log level before setting
    validated_level = validate_log_level(settings.LOG_LEVEL)
    logger.setLevel(validated_level)
    logger.info("Starting cohort generation process")

    ensure_population_file_exists()

    # Use string cache for the entire process
    with pl.StringCache():
        container = get_container()

        output_dir = output_dir or settings.COHORT_FILE.parent
        os.makedirs(output_dir, exist_ok=True)

        try:
            # Initialize container services
            container.initialize()  # Add this line to initialize all services

            # Prepare register configurations
            register_configs = create_register_configs()
            logger.info("Register configurations created")

            # Configure register service
            container.get_register_service().configure({"registers": register_configs})
            logger.info("Register service configured")

            # Configure population service first
            container.get_population_service().configure(
                {
                    "bef_files": str(settings.BEF_FILES),
                    "mfr_files": str(settings.MFR_FILES),
                    "population_file": str(settings.POPULATION_FILE),
                    "birth_inclusion_start_year": settings.BIRTH_INCLUSION_START_YEAR,
                    "birth_inclusion_end_year": settings.BIRTH_INCLUSION_END_YEAR,
                }
            )
            logger.info("Population service configured")

            # Ensure the population file is generated
            population_service = container.get_population_service()
            population_result = population_service.process_population()
            population_result.collect().write_parquet(settings.POPULATION_FILE)
            logger.info(f"Population file generated at: {settings.POPULATION_FILE}")

            # Configure cohort service
            container.get_cohort_service().configure(
                {
                    "lpr2_path": {
                        "adm": str(settings.LPR_ADM_FILES),
                        "diag": str(settings.LPR_DIAG_FILES),
                        "bes": str(settings.LPR_BES_FILES),
                    },
                    "lpr3_path": {
                        "kontakter": str(settings.LPR3_KONTAKTER_FILES),
                        "diagnoser": str(settings.LPR3_DIAGNOSER_FILES),
                    },
                    "population_file": str(settings.POPULATION_FILE),
                }
            )
            logger.info("Cohort service configured")

            # Configure analytical data service
            container.get_analytical_data_service().configure(
                {
                    "output_base_path": str(output_dir / "analytical_data"),
                    "population_file": str(settings.POPULATION_FILE),
                    "stage_results": {},  # This will be populated during pipeline execution
                }
            )
            logger.info("Analytical data service configured")

            # Configure table service
            container.get_table_service().configure(
                {
                    "output_dir": str(output_dir / "tables"),
                    "study_years": [2005, 2010, 2015, 2020],
                    "analytical_data_path": str(output_dir / "analytical_data"),
                }
            )
            logger.info("Table service configured")

            # # Configure statistics service
            # try:
            #     statistics_config = configure_statistics({"output_dir": str(output_dir) if output_dir else "."})
            #     container.get_statistics_service().configure(statistics_config)
            # except Exception as e:
            #     logger.error(f"Error configuring statistics service: {e}")
            #     raise

            # Configure pipeline service last
            container.get_pipeline_service().configure(
                {
                    "stage_order": [
                        "population",
                        "health",
                        "bef_longitudinal",
                        "uddf_longitudinal",
                        "ind_longitudinal",
                        "akm_longitudinal",
                    ],
                    "register_configs": register_configs,
                    "output_configs": {
                        "final_cohort": str(settings.COHORT_FILE),
                        "analytical_data": {
                            "base_path": str(output_dir / "analytical_data"),
                            "domains": {
                                "demographics": {"sources": ["bef_longitudinal"], "temporal": True},
                                "education": {"sources": ["uddf_longitudinal"], "temporal": True},
                                "income": {"sources": ["ind_longitudinal"], "temporal": True},
                                "employment": {"sources": ["akm_longitudinal"], "temporal": True},
                            },
                        },
                    },
                    "stage_configs": {
                        "population": {
                            "name": "population",
                            "output_file": str(settings.POPULATION_FILE),
                        },
                        "health": {
                            "name": "health",
                            "depends_on": ["population"],
                            "output_file": str(output_dir / "health.parquet"),
                        },
                        "bef_longitudinal": {
                            "name": "bef_longitudinal",
                            "depends_on": ["health"],
                            "register_name": "bef",
                            "output_file": str(output_dir / "bef_longitudinal.parquet"),
                        },
                        "uddf_longitudinal": {
                            "name": "uddf_longitudinal",
                            "depends_on": ["bef_longitudinal"],
                            "register_name": "uddf",
                            "output_file": str(output_dir / "uddf_longitudinal.parquet"),
                        },
                        "ind_longitudinal": {
                            "name": "ind_longitudinal",
                            "depends_on": ["uddf_longitudinal"],
                            "register_name": "ind",
                            "output_file": str(output_dir / "ind_longitudinal.parquet"),
                        },
                        "akm_longitudinal": {
                            "name": "akm_longitudinal",
                            "depends_on": ["ind_longitudinal"],
                            "register_name": "akm",
                            "output_file": str(output_dir / "akm_longitudinal.parquet"),
                        },
                    },
                }
            )
            logger.info("Pipeline service configured")

            # Run pipeline
            logger.info("Starting pipeline execution")
            results = container.get_pipeline_service().run_pipeline()

            if not results:
                raise ValueError("Pipeline execution produced no results")

            # Get the final result
            final_result = container.get_pipeline_service().get_final_result(results)

            # Generate tables
            try:
                logger.info("Generating descriptive statistics tables")
                table_service = container.get_table_service()

                # Generate unstratified table
                tables = table_service.create_table_one()  # No need to pass data
                table_service.save_tables(tables, prefix="table_one_unstratified")

                # Generate sex-stratified table
                stratified_tables = table_service.create_table_one(stratify_by="sex")  # Using standardized column name
                table_service.save_tables(stratified_tables, prefix="table_one_stratified_by_sex")

                logger.info("Tables generated successfully")

            except Exception as e:
                logger.error(f"Error generating tables: {str(e)}", exc_info=True)
                # Continue with the main process even if table generation fails

            # Write final result if output_dir is provided
            if output_dir:
                final_output_path = output_dir / "final_cohort.parquet"
                logger.info(f"Writing final cohort to: {final_output_path}")
                container.get_data_service().write_parquet(final_result, final_output_path)

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise

        finally:
            # Clean up resources
            container.shutdown()
            logger.info("Services shut down")

        # Return the final result outside the try-finally block
    return final_result


if __name__ == "__main__":
    result = main()
    # Optionally collect and write the result here
    result.collect().write_parquet("final_output.parquet")
