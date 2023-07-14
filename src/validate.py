import sys
import os
sys.path.append(os.path.abspath("."))
from great_expectations.core import ExpectationSuite
from great_expectations.core import ExpectationConfiguration
from config_reader import Config

conf = Config()

def validator() -> ExpectationSuite:
    """
    Builds and Retrive Data Validator of Expectation Suite
    """
    energy_consumption_expectation_suite = ExpectationSuite(expectation_suite_name = "energy_consumption")
    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
        expectation_type = "expect_table_columns_to_match_ordered_list",
        kwargs = {
            "columns_list":["HourUTC", "PriceArea", "ConsumerType_DE35", "TotalCon"]
        }
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
        expectation_type = "expect_table_column_count_to_equal",
        kwargs = {"value":4}
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
        expectation_type = "expect_column_values_not_to_be_null",
        kwargs = {
            "column": "HourUTC"
        }
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": "PriceArea", "value_set": (0, 1, 2)}
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "PriceArea", "type_": "int8"},
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": "ConsumerType_DE35", "value_set": conf.api["consumer_type_unique_values"]}
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "ConsumerType_DE35", "type_": "int16"},
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "TotalCon", "min_value": 0, "strict_min":False},
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "TotalCon", "type_": "int64"},
        )
    )

    energy_consumption_expectation_suite.add_expectation(
        ExpectationConfiguration(
        expectation_type = "expect_column_values_not_to_be_null",
        kwargs = {
            "column": "TotalCon"
        }
        )
    )

    return energy_consumption_expectation_suite