import pytest
from pyspark.sql import SparkSession
from utils.data_preparation import PreparationValidationData
from chispa import assert_df_equality

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark_session = (
        SparkSession.builder.appName("TestDataPrep").master("local[*]").getOrCreate()
    )
    spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    yield spark_session
    spark_session.stop()


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
    cleaned_df.show()
    expected_df.show()
    assert_df_equality(cleaned_df, expected_df)


def test_data_enforce_type(data_prep, spark):
    # Sample data
    data = [
        ("2023-12-01", "123", "2023/12/02"),
        ("12/02/2023", "456", "2023-12-03"),
    ]

    # Create sample  DataFrame
    schema = StructType(
        [
            StructField("transaction_date", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("alternate_date", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    # Schema to enforce
    desired_schema = StructType(
        [
            StructField("transaction_date", DateType(), True),
            StructField("amount", IntegerType(), True),
            StructField("alternate_date", DateType(), True),
        ]
    )

    # Apply the data_enforce_type method
    result_df = data_prep.data_enforce_type(df, desired_schema)

    # Define the expected DataFrame with enforced data types
    expected_data = [
        (
            spark.sql("SELECT to_date('2023-12-01', 'yyyy-MM-dd')").collect()[0][0],
            123,
            spark.sql("SELECT to_date('2023/12/02', 'yyyy/MM/dd')").collect()[0][0],
        ),
        (
            spark.sql("SELECT to_date('12/02/2023', 'MM/dd/yyyy')").collect()[0][0],
            456,
            spark.sql("SELECT to_date('2023-12-03', 'yyyy-MM-dd')").collect()[0][0],
        ),
    ]

    expected_schema = StructType(
        [
            StructField("transaction_date", DateType(), True),
            StructField("amount", IntegerType(), True),
            StructField("alternate_date", DateType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Assert the result DataFrame matches the expected DataFrame
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
