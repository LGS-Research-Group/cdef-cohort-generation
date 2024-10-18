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
python -m cdef_cohort_builder.main
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
- LPR3 diagnoser / LPR3 kontakter directory names (OBS. convert script)
- Mapping and .env file not included in repo
- SENR not available in early years for AKM

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Contributors

- Tobias Kragholm

## Acknowledgments

This project uses data from Danish national registers and is conducted in compliance with Danish data protection regulations.

## Documentation

### Configuration through environment variables

The project uses environment variables for configuration. The following environment variables are used:

### Core functions

#### `process_register_data`

The `process_register_data` function handles the `longitudinal` and `join_parents_only` flags in the following ways:

1. Longitudinal flag:

   - If `longitudinal` is True:
     - The function processes each input file separately, adding 'year' and 'month' columns based on the filename.
     - It applies column validation and selection to each file individually.
     - All processed data frames are concatenated at the end.
   - If `longitudinal` is False:
     - The function reads all input files at once using `pl.scan_parquet(files, ...)`.
     - Column validation and selection are applied to the entire dataset.

2. Join_parents_only flag:

   This flag is used when joining the processed data with the population data (if a population file is provided). It's only relevant for non-longitudinal data processing.

   - If `join_parents_only` is True:
     - The function creates separate datasets for father's and mother's data.
     - It prefixes all columns (except the join column) with 'FAR_' for father's data and 'MOR_' for mother's data.
     - It then joins these datasets with the population data using 'FAR_ID' and 'MOR_ID' respectively.
   - If `join_parents_only` is False:
     - The function performs a simple join between the processed data and the population data using the specified `join_on` column(s).

Key points:

- The `longitudinal` flag primarily affects how the input files are read and processed (individually or all at once).
- The `join_parents_only` flag is only relevant when there's a population file to join with, and it determines whether to create separate parent-specific datasets or not.
- Both flags work independently of each other, allowing for different combinations of longitudinal/non-longitudinal processing with or without parent-specific joining.

This design allows for flexibility in handling different types of register data and joining requirements.

#### UDDF

When the `longitudinal` flag is set to True and the register being processed is UDDF (Danish Education Register), the processing works as follows:

1. The `process_register_data` function in `generic.py` is called with the `longitudinal` parameter set to True.

2. Instead of reading all files into a single dataframe, each file is processed separately:
   - Each .parquet file in the UDDF_FILES directory is read individually.
   - Year and month information is extracted from each filename and added as new columns to the dataframe.
   - If specified, only the columns listed in `columns_to_keep` are retained.

3. All these individual dataframes are then concatenated into a single large dataframe using `pl.concat(data_frames)`.

4. Date columns (HF_VFRA and HF_VTIL) are parsed and converted to the appropriate date format.

5. Special handling for UDDF:
   - ISCED (International Standard Classification of Education) data is read and joined with the UDDF data based on the HFAUDD column.

6. The data is then joined with the population file:
   - Since `join_parents_only` is True for UDDF, the data is joined twice - once for fathers and once for mothers.
   - The columns from UDDF are prefixed with "FAR_" for fathers and "MOR_" for mothers, except for the join column (PNR).

7. The resulting dataframe includes:
   - All columns from the population file
   - UDDF data for fathers (prefixed with FAR_)
   - UDDF data for mothers (prefixed with MOR_)
   - Year and month columns indicating when each record was valid

8. Finally, the processed data is written to the output file (UDDF_OUT) in Parquet format.

This longitudinal processing allows for tracking changes in education over time for both parents, with each record associated with a specific year and month.
