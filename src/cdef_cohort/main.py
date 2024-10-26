import logging
import os
from pathlib import Path

from cdef_cohort.logging_config import logger
from cdef_cohort.services.cohort_service import CohortService
from cdef_cohort.services.container import get_container
from cdef_cohort.settings import settings  # Add settings import
from cdef_cohort.utils.config import (
    BEF_FILES,
    BIRTH_INCLUSION_END_YEAR,
    BIRTH_INCLUSION_START_YEAR,
    CHILD_EVENT_DEFINITIONS,
    COHORT_FILE,
    LPR3_DIAGNOSER_OUT,
    LPR3_KONTAKTER_OUT,
    LPR_ADM_OUT,
    LPR_BES_OUT,
    LPR_DIAG_OUT,
    MFR_FILES,
    POPULATION_FILE,
    STATIC_COHORT,
)

logging.getLogger("polars").setLevel(logging.WARNING)


def main(output_dir: Path | None = None) -> None:
    # Set logging level from settings
    logger.setLevel(settings.LOG_LEVEL.upper())
    logger.info("Starting cohort generation process")

    # Get service container
    container = get_container()

    # Set default output directory if none provided
    output_dir = output_dir or COHORT_FILE.parent
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Configure all services first
        container.config.configure(
            {
                "mappings_path": Path("mappings"),
                "register_configs": {
                    "population": {
                        "input_files": str(POPULATION_FILE),
                        "output_file": str(COHORT_FILE),
                        "schema_def": {},
                        "defaults": {},
                    }
                },
            }
        )

        # Initialize all services
        container.initialize()

        # Configure population service
        container.population_service.configure(
            {
                "bef_files": BEF_FILES,
                "mfr_files": MFR_FILES,
                "population_file": POPULATION_FILE,
                "birth_inclusion_start_year": BIRTH_INCLUSION_START_YEAR,
                "birth_inclusion_end_year": BIRTH_INCLUSION_END_YEAR,
            }
        )

        # Process population data
        logger.info("Processing population data")
        container.population_service.process_population()

        # Configure cohort service
        cohort_service = CohortService(
            data_service=container.data_service,
            event_service=container.event_service,
            mapping_service=container.mapping_service
        )

        cohort_service.configure(
            {
                "population_file": POPULATION_FILE,
                "output_dir": output_dir,
                "lpr2_path": {
                    "adm": LPR_ADM_OUT,
                    "diag": LPR_DIAG_OUT,
                    "bes": LPR_BES_OUT,
                },
                "lpr3_path": {
                    "kontakter": LPR3_KONTAKTER_OUT,
                    "diagnoser": LPR3_DIAGNOSER_OUT,
                },
            }
        )

        # Process cohort data
        logger.info("Processing severe chronic disease data")
        scd_data = cohort_service.identify_severe_chronic_disease()

        logger.info("Processing static cohort data")
        static_cohort = cohort_service.process_static_data(scd_data)
        container.data_service.write_parquet(static_cohort, STATIC_COHORT)

        logger.info("Processing events")
        cohort_service.process_events(
            static_cohort,
            CHILD_EVENT_DEFINITIONS,
            output_dir / "child_events.parquet"
        )

        logger.info("Cohort generation process completed successfully")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise

    finally:
        container.shutdown()
        logger.info("Services shut down")


if __name__ == "__main__":
    main()
