import unittest

import polars as pl

from cdef_cohort_generation.education import process_education_data, read_isced_data


class TestEducationProcessing(unittest.TestCase):
    def test_read_isced_data(self):
        isced_data = read_isced_data()
        self.assertIsInstance(isced_data, pl.DataFrame)
        self.assertIn("EDU_TYPE", isced_data.columns)
        self.assertIn("EDU_LVL", isced_data.columns)

    def test_process_education_data(self):
        # Create a sample education DataFrame
        edu_data = pl.DataFrame(
            {
                "PNR": ["1", "1", "2", "2"],
                "EDU_LVL": ["3", "4", "5", "6"],
                "EDU_TYPE": ["A", "B", "C", "D"],
                "EDU_DATE": ["2000-01-01", "2005-01-01", "2010-01-01", "2015-01-01"],
            },
        )
        processed_data = process_education_data(edu_data)
        self.assertEqual(len(processed_data), 2)  # Two unique PNRs
        self.assertEqual(processed_data.filter(pl.col("PNR") == "1")["highest_edu_level"][0], 4)
        self.assertEqual(processed_data.filter(pl.col("PNR") == "2")["highest_edu_level"][0], 6)

    # Add more unit tests as needed


if __name__ == "__main__":
    unittest.main()
