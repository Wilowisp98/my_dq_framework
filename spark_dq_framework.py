import sys
import logging
import json
from pyspark.sql import SparkSession, functions as F
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from time import time

# -----------------------------------
# CONFIGURE SPARK SESSION
# -----------------------------------
spark = SparkSession.builder.appName("myDQ_TOOL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------
# CONFIGURE LOGGING
# -----------------------------------
def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application."""
    format_str = "%(asctime)s %(levelname)s %(name)s.%(funcName)s(%(lineno)d): %(message)s"
    date_format = "%y/%m/%d %H:%M:%S"

    logging.basicConfig(
        format=format_str, 
        datefmt=date_format,
        level=level,
        stream=sys.stdout
    )
    
logger = logging.getLogger(__name__)
configure_logging()

# -----------------------------------
# MAIN CLASS
# -----------------------------------

class SparkDQTesting:
    """
    A class for performing DQ tests on Spark tables.
    
    Attributes:
        table_schema (str): The schema/database name containing the table
        table_name (str): The name of the table to test
        key_columns (List[str]): List of columns that should form a unique key
        lud_column (Optional[str]): Last update date column name
        partition (Optional[Dict[str, Union[str, List[str]]]]): Partition information
        table_df (DataFrame): Spark DataFrame containing the table data
        table_columns (List[str]): List of all columns in the table
        testing_results (Dict[str, Dict]): Dictionary containing test results
    """
    
    def __init__(
        self, 
        table_schema: str, 
        table_name: str, 
        key_columns: List[str], 
        lud_column: Optional[str] = None, 
        partition: Optional[Dict[str, Union[str, List[str]]]] = None
    ):
        """Initialize the testing class with table information."""
        self._validate_inputs(table_schema, table_name, key_columns)
        
        self.table_schema = table_schema
        self.table_name = table_name
        self.key_columns = key_columns
        self.lud_column = lud_column
        self.partition = partition
        self.testing_results = {}
        
        self._load_table_data()

    # -----------------------------------
    # CONFIGURATION VALIDATION
    # -----------------------------------

    def _validate_inputs(self, table_schema: str, table_name: str, key_columns: List[str]) -> None:
        """Validate input parameters."""
        if not all(isinstance(x, str) for x in [table_schema, table_name]):
            raise ValueError("table_schema and table_name must be strings")
        if not isinstance(key_columns, list) or not all(isinstance(x, str) for x in key_columns):
            raise ValueError("key_columns must be a list of strings")

    # -----------------------------------
    # LOADING DATA
    # -----------------------------------

    def _build_query(self) -> str:
        """Build the SQL query for loading table data."""
        query = f"SELECT * FROM {self.table_schema}.{self.table_name}"
        
        if self.partition:
            key, value = next(iter(self.partition.items()))
            if isinstance(value, list):
                partitions = ','.join(map(str, value))
                query += f" WHERE {key} in ({partitions})"
            else:
                query += f" WHERE {key} in ({value})"
                
        return query

    def _load_table_data(self) -> None:
        """Load table data into DataFrame."""
        try:
            query = self._build_query()
            self.table_df = spark.sql(query)
            self.table_columns = self.table_df.columns

            self.table_count = self.table_df.count()
            if self.table_count == 0:
                raise ValueError('Table contains no data')
            
        except Exception as e:
            raise RuntimeError(f"Failed to query table: {str(e)}")

    # -----------------------------------
    # TESTS
    # -----------------------------------

    def _create_test_result(self, status: str, details: Dict[str, Any]) -> Dict[str, Any]:
        """Create a standardized test result dictionary."""
        return {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
    
    def find_duplicates(self) -> None:
        """Check for duplicate records based on key columns."""
        try:
            duplicates = self.table_df.groupBy(self.key_columns) \
                        .count() \
                        .where('count > 1')
            
            duplicate_count = duplicates.count()
            
            self.testing_results['find_duplicates'] = self._create_test_result(
                status='Passed' if duplicate_count == 0 else 'Failed',
                details={'duplicated_rows_count': duplicate_count}
            )
            
        except Exception as e:
            self.testing_results['find_duplicates'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def find_nulls(self) -> None:
        """Check for null records in key columns."""
        try:
            null_details = {
                column: self.table_df.where(F.col(column).isNull()).count() for column in self.key_columns
            }
            
            nulls_sum = sum(null_details.values())
            
            self.testing_results['find_nulls'] = self._create_test_result(
                status='Passed' if nulls_sum == 0 else 'Failed',
                details={
                    'total_null_count': nulls_sum,
                    'nulls_by_column': null_details
                }
            )
            
        except Exception as e:
            self.testing_results['find_nulls'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def check_completeness(self) -> None:
        """Check for null records in non-key columns."""
        try:
            non_key_columns = list(set(self.table_columns) - set(self.key_columns))
            
            if not non_key_columns:
                self.testing_results['check_completeness'] = self._create_test_result(
                    status='Skipped',
                    details='Every column is a key column'
                )
                return
                
            null_details = {}
            status = 'Passed'

            for column in non_key_columns:
                null_percentage = (self.table_df.where(F.col(column).isNull()).count() / self.table_count) * 100
                null_details[column] = f'{int(null_percentage)}%'
                if null_percentage > 5:
                    status = 'Failed'

            self.testing_results['check_completeness'] = self._create_test_result(
                status=status,
                details={'nulls_percentage_by_column': null_details}
            )
            
        except Exception as e:
            self.testing_results['check_completeness'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def values_range(self) -> None:
        """Check the values range of numeric columns."""
        try:
            range_details = {}
            non_key_columns = list(set(self.table_columns) - set(self.key_columns))

            for column in non_key_columns:
                try:
                    stats = self.table_df.select(
                        F.max(column).alias('max'),
                        F.min(column).alias('min'),
                        F.avg(column).alias('avg')
                    ).first()
                    
                    # Try to convert max value to float to check if numeric
                    float(stats['max'])
                    
                    range_details[column] = {
                        'max': str(stats['max']),
                        'min': str(stats['min']),
                        'avg': str(stats['avg'])
                    }
                except (ValueError, TypeError):
                    continue

            self.testing_results['values_range'] = self._create_test_result(
                status='Passed',
                details={'values_range_details': range_details}
            )
            
        except Exception as e:
            self.testing_results['values_range'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def key_density(self) -> None:
        """Check the density and distribution of key columns."""
        try:
            key_density = {}

            for column in self.key_columns:
                value_counts = self.table_df.groupBy(column).count()
                stats = value_counts.agg(
                    F.max('count').alias('max_count'),
                    F.min('count').alias('min_count'),
                    F.avg('count').alias('avg_count'),
                    F.count('count').alias('distinct_values')
                ).first()

                max_value = value_counts.where(F.col('count') == stats['max_count']).first()
                min_value = value_counts.where(F.col('count') == stats['min_count']).first()

                key_density[column] = {
                    'max_count': {
                        'value': str(max_value[column]) if max_value else None,
                        'count': str(stats['max_count'])
                    },
                    'min_count': {
                        'value': str(min_value[column]) if min_value else None,
                        'count': str(stats['min_count'])
                    },
                    'avg_count': str(stats['avg_count']),
                    'distinct_values': str(stats['distinct_values']),
                    'total_records': str(self.table_df.count())
                }

            self.testing_results['key_density'] = self._create_test_result(
                status='Passed',
                details={'key_density_details': key_density}
            )
            
        except Exception as e:
            self.testing_results['key_density'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def lud_density(self) -> None:
        """Check the density and distribution of the last update date column."""
        if not self.lud_column:
            return
            
        try:
            value_counts = self.table_df.groupBy(self.lud_column).count()
            stats = value_counts.agg(
                F.max('count').alias('max_count'),
                F.min('count').alias('min_count'),
                F.avg('count').alias('avg_count'),
                F.count('count').alias('distinct_values')
            ).first()

            max_value = value_counts.where(F.col('count') == stats['max_count']).first()
            min_value = value_counts.where(F.col('count') == stats['min_count']).first()

            lud_density_details = {
                'max_count': {
                    'value': str(max_value[self.lud_column]) if max_value else None,
                    'count': str(stats['max_count'])
                },
                'min_count': {
                    'value': str(min_value[self.lud_column]) if min_value else None,
                    'count': str(stats['min_count'])
                },
                'avg_count': str(float(stats['avg_count'])),
                'distinct_values': str(stats['distinct_values'])
            }

            ordered_counts = value_counts.orderBy(F.col(self.lud_column).desc()).collect()

            if len(ordered_counts) >= 7:
                last_luds = [row['count'] for row in ordered_counts[1:7]]
                last_luds_avg = sum(last_luds) / len(last_luds)
                current_count = ordered_counts[0]['count']
                ratio = current_count / last_luds_avg if last_luds_avg > 0 else 0
                below_acceptable = ratio < 0.4

                lud_density_details['moving_average'] = {
                    'last_luds_avg': str(float(last_luds_avg)),
                    'current_count': str(current_count),
                    'ratio_to_average': str(ratio),
                    'below_acceptable_average': below_acceptable,
                    'dates_analyzed': {
                        'current_date': str(ordered_counts[0][self.lud_column]),
                        'analyzed_dates': [str(row[self.lud_column]) for row in ordered_counts[1:7]]
                    }
                }
            else:
                lud_density_details['moving_average'] = {
                    'error': 'Insufficient data for moving average calculation'
                }

            self.testing_results['lud_density'] = self._create_test_result(
                status='Failed' if lud_density_details.get('moving_average', {}).get('below_acceptable_average', True) else 'Passed',
                details=lud_density_details
            )
            
        except Exception as e:
            self.testing_results['lud_density'] = self._create_test_result(
                status='Error',
                details=str(e)
            )

    def run_all_tests(self) -> Dict[str, Dict]:
        """Run all available tests and return results."""
        test_methods = [
            ('find_duplicates', self.find_duplicates),
            ('find_nulls', self.find_nulls),
            ('check_completeness', self.check_completeness),
            ('values_range', self.values_range),
            ('key_density', self.key_density),
            ('lud_density', self.lud_density)
        ]

        for test_name, test_method in test_methods:
            logger.info(f'Executing {test_name}...')
            test_method()

        return self.testing_results

def main():
    """Main entry point of the application."""
    try:
        config = {
            'table_schema': "[TABLE_SCHEMA]",
            'table_name': "[TABLE_NAME]",
            'key_columns': ["KEY_1", "KEY_2", "KEY_3"],
            'lud_column': "LAST_UPDATE_DATE_COLUMN",
            'partition': {'PARTITION_KEY': 'PARTITION_VALUE'}
        }

        start_time = time()
        unit_tests = SparkDQTesting(**config)
        results = unit_tests.run_all_tests()
        end_time = time()

        print('\n')
        logger.info(f'DQ Testing Tool finished after {end_time - start_time:.2f} seconds.\n')
        print(json.dumps(results, indent=4))
        
    except Exception as e:
        raise Exception(f'An error occurred while executing the DQ tests: {str(e)}')

if __name__ == '__main__':
    main()