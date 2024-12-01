import pytest
from pyspark.sql import SparkSession
from utils.data_preparation import PreparationValidationData
from chispa import assert_df_equality

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


@pytest.fixture(scope="module")
def spark():
    """
    Create and return a SparkSession for the tests
    """

    return SparkSession.builder.appName("TestSession").getOrCreate()


@pytest.fixture
def data_prep(spark):
    """
    Create an instance to test the PreparationValidationData class
    """

    return PreparationValidationData(spark)


def test_data_handle_nulls(data_prep, spark):
    """
    Test handling nulls by dropping rows
    """
    data = [("A", 1), ("B", None), (None, 3)]
    df = spark.createDataFrame(data, ["name", "value"])
    cleaned_df = data_prep.data_handle_nulls(df, strategy="drop")

    # Expected DataFrame
    expected_data = [("A", 1)]
    expected_df = spark.createDataFrame(expected_data, ["name", "value"])
    assert_df_equality(cleaned_df, expected_df)


def test_data_drop_duplicates_flag(data_prep, spark):
    """
    Test dropping duplicate rows
    """
    data = [("A", 1), ("A", 1), ("B", 2)]
    df = spark.createDataFrame(data, ["name", "value"])
    df_deduped = data_prep.data_drop_duplicates(
        df, subset=["name", "value"], action="flag"
    )

    # Expected DataFrame
    expected_data = [("A", 1, False), ("B", 2, False)]
    expected_df = spark.createDataFrame(
        expected_data, ["name", "value", "is_duplicate"]
    )
    assert_df_equality(df_deduped, expected_df)


def test_data_enforce_type(data_prep, spark):
    """
    Test enforcing data types
    """
    data = [("A", "1"), ("B", "2")]
    df = spark.createDataFrame(data, ["name", "value"])
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )
    enforced_df = data_prep.data_enforce_type(df, schema)

    # Expected DataFrame
    expected_data = [("A", 1), ("B", 2)]
    expected_df = spark.createDataFrame(expected_data, ["name", "value"])
    assert_df_equality(enforced_df, expected_df)
