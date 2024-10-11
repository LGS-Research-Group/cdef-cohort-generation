# cdef-cohort-generation

This Python project is designed to process and analyze data from Danish national registers for an observational study investigating the long-term impact of severe chronic diseases in children on parental income trajectories in Denmark.

## Project Overview

The primary objectives of this study are:

1. Quantify the difference in total personal income between parents of children with severe chronic diseases and matched controls over a 22-year period (2000-2022).
2. Explore how this impact varies across disease severity, geographical location, and parental education levels.
3. Examine gender differences in the economic impact of childhood chronic diseases on parents.
4. Assess the role of socioeconomic factors in moderating the impact of childhood chronic diseases on parental income trajectories.

## Key Features

- Process and combine data from various Danish national registers
- Identify severe chronic diseases using ICD-10 codes
- Generate cohorts for analysis
- Perform longitudinal data analysis
- Apply statistical methods including difference-in-differences analysis and marginal structural models

## Project Structure

- `src/cdef_cohort_generation/`: Main package containing all the source code
  - `config.py`: Configuration settings and file paths
  - `logging_config.py`: Logging setup
  - `main.py`: Main execution script
  - `mappings.py`: Data mappings and dictionaries
  - `population.py`: Population data processing
  - `registers/`: Modules for processing different registers
  - `types.py`: Custom type definitions
  - `utils.py`: Utility functions

- `tests/`: Unit tests for the project

## Installation

This project requires Python 3.12.6 and uses `rye` for dependency management.

1. Clone the repository
2. Install `rye` if you haven't already (see [here](https://github.com/astral-sh/rye#installation))
3. Navigate to the project directory and set up the environment:
   ```
   rye sync
   ```

## Usage

To run the main processing script:

```
python -m cdef_cohort_generation.main
```

## Testing

To run the unit tests:

```
pytest tests/
```

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Contributors

- Tobias Kragholm

## Acknowledgments

This project uses data from Danish national registers and is conducted in compliance with Danish data protection regulations.
