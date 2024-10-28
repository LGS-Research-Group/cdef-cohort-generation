from pathlib import Path

from cdef_cohort.config.examples.standard_config import create_standard_config
from cdef_cohort.services.container import get_container


def main():
    # Get service container
    container = get_container()

    # Configure services using standard config
    base_dir = Path("/path/to/data")
    config = create_standard_config(base_dir)

    # Configure all services at once
    container.configure(config)

    # Initialize all services
    container.initialize()

    try:
        # Run pipeline
        results = container.get_pipeline_service().run_pipeline()

        # Get final cohort
        final_cohort = container.get_pipeline_service().get_final_result(results)

        # Write results
        container.get_data_service().write_parquet(final_cohort, base_dir / "final_cohort.parquet")

        # # Read static individual data
        # individuals = pl.read_parquet("analytical_data/static/individual_attributes.parquet")

        # # Read specific year of demographic data
        # demographics_2010 = pl.read_parquet("analytical_data/longitudinal/demographics/year=2010")

        # # Read family relationships
        # families = pl.read_parquet("analytical_data/family/family_relationships.parquet")

        # # Combine for analysis
        # analysis_df = (
        #     individuals
        #     .join(families, on="individual_id")
        #     .join(demographics_2010, on="individual_id")
        # )

    finally:
        # Clean up
        container.shutdown()


if __name__ == "__main__":
    main()
