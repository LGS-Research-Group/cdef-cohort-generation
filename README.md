# cdef-cohort-generation

This Python project is part of a research study conducted by the [Center for Data og Effektforskning (CDEF)](https://www.rigshospitalet.dk/maryelizabethshospital/center-for-data-og-effektforskning/Sider/default.aspx) at Mary Elizabeths Hospital, Denmark. The project aims to generate cohorts for an observational study investigating the long-term impact of severe chronic diseases in children on parental income trajectories in Denmark.

The package has been built in such a way that it can be easily extended to include additional registers and data sources. The project is designed to be modular and flexible, allowing for easy integration of new data sources and registers. And also to be easily adaptable to other research projects that require processing and analysis of Danish national registers. But this package focuses on creating the initial cohort/population for the study.

## Project Overview

This project is designed to process and analyze data from Danish national registers for an observational study investigating the long-term impact of severe chronic diseases in children on parental income trajectories in Denmark.

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

## Registers implemented

### Registers from Sundhedsdatastyrelsen

- LPR_ADM: Administrative data from hospitals (LPR2)
- LPR_DIAG: Diagnoses from hospitals (LPR2)
- LPR_BES: Outpatient visits from hospitals (LPR2)
- LPR_KONTAKER: Contacts with hospitals (LPR3)
- LPR_DIAGNOSER: Diagnoses from hospitals (LPR3)

### Registers from Statistics Denmark

- BEF: Population data
- IND: Income data
- IDAN: IDA employment data
- UDDF: Education data
- AKM: Work classification module

## Testing

To run the unit tests:

```
pytest tests/
```

## Todo

- Make sure LPR2/LPR3 processing is as smooth as possible
- Include mappings for variables + ISCED
- Improve logging and error handling
- Add descriptive plots
- Refactor code for better organization and efficiency

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Contributors

- Tobias Kragholm

## Acknowledgments

This project uses data from Danish national registers and is conducted in compliance with Danish data protection regulations.
