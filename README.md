# Data Quality Testing Tool

I'm lazy and running the exact same tests everytime seemed way to boring. This is a Python application built using Spark that provides a framework for performing data quality testing on tables.

## Data Quality Tests

The tool includes the following tests:
  - **Duplicate Records Check**: Identifies duplicate records based on the specified key columns.
  - **Null Values Check**: Checks for null values in key columns.
  - **Completeness Check**: Checks for null values in non-key columns.
  - **Values Range Check**: Evaluates the range and distribution of numeric column values.
  - **Key Density Check**: Analyzes the density and distribution of key column values.
  - **Last Update Date (LUD) Density Check**: Analyzes the density and distribution based on the last update date column.

## Usage

1. Install the required dependencies:
   - Apache Spark
   - Python (3.6 or higher)
   - Logging and other standard Python libraries

2. Update the configuration dictionary in the `main()` function with the appropriate values for your use case:
   - `table_schema`: The schema/database name containing the table
   - `table_name`: The name of the table to test
   - `key_columns`: List of columns that should form a unique key
   - `lud_column`: (Optional) The last update date column name
   - `partition`: (Optional) Partition information in the form of a dictionary

3. Run the `main()` function to execute the data quality tests. The results will be printed to the console in JSON format.

## Example Output

```json
{
    "find_duplicates": {
        "status": "Passed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "duplicated_rows_count": 0
        }
    },
    "find_nulls": {
        "status": "Failed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "total_null_count": 25,
            "nulls_by_column": {
                "key_column_1": 10,
                "key_column_2": 15
            }
        }
    },
    "check_completeness": {
        "status": "Passed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "nulls_percentage_by_column": {
                "non_key_column_1": "3%",
                "non_key_column_2": "1%"
            }
        }
    },
    "values_range": {
        "status": "Passed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "values_range_details": {
                "numeric_column_1": {
                    "max": "1000.0",
                    "min": "0.0",
                    "avg": "500.0"
                },
                "numeric_column_2": {
                    "max": "100.0",
                    "min": "10.0",
                    "avg": "50.0"
                }
            }
        }
    },
    "key_density": {
        "status": "Passed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "key_density_details": {
                "key_column_1": {
                    "max_count": {
                        "value": "ABC",
                        "count": "50"
                    },
                    "min_count": {
                        "value": "XYZ",
                        "count": "1"
                    },
                    "avg_count": "10.0",
                    "distinct_values": "100",
                    "total_records": "500"
                },
                "key_column_2": {
                    "max_count": {
                        "value": "2022-01-01",
                        "count": "100"
                    },
                    "min_count": {
                        "value": "2022-01-02",
                        "count": "5"
                    },
                    "avg_count": "20.0",
                    "distinct_values": "30",
                    "total_records": "500"
                }
            }
        }
    },
    "lud_density": {
        "status": "Failed",
        "timestamp": "2023-08-15T12:34:56.789012",
        "details": {
            "max_count": {
                "value": "2023-08-01",
                "count": "100"
            },
            "min_count": {
                "value": "2023-07-01",
                "count": "10"
            },
            "avg_count": "50.0",
            "distinct_values": "30",
            "moving_average": {
                "last_luds_avg": "70.0",
                "current_count": "100",
                "ratio_to_average": "1.4",
                "below_acceptable_average": false,
                "dates_analyzed": {
                    "current_date": "2023-08-01",
                    "analyzed_dates": [
                        "2023-08-01",
                        "2023-07-31",
                        "2023-07-30",
                        "2023-07-29",
                        "2023-07-28",
                        "2023-07-27"
                    ]
                }
            }
        }
    }
}
```
