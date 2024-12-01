import pytest
from pyspark.sql import SparkSession, functions as F
from utils.data_preparation import PreparationValidationData
from chispa import assert_df_equality
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)


# Fixture to create a SparkSession for testing
@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark_session = (
        SparkSession.builder.appName("TestDataPrep").master("local[*]").getOrCreate()
    )
    spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    yield spark_session
    spark_session.stop()


# Fixture to create an instance of the PreparationValidationData class
@pytest.fixture
def data_prep(spark):
    """Create an instance to test the PreparationValidationData class."""
    return PreparationValidationData(spark)


# Helper function to create a DataFrame from data and schema
def create_test_df(spark, data, schema):
    """Helper function to create a DataFrame."""
    return spark.createDataFrame(data, schema)


# Test case for handling null values by dropping rows
def test_data_handle_nulls(data_prep, spark):
    """Test the data handling for nulls by dropping rows."""
    data = [("A", 1), ("B", None), (None, 3)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Clean the DataFrame using the drop strategy for nulls
    cleaned_df = data_prep.data_handle_nulls(df, strategy="drop")

    # Expected DataFrame after dropping rows with nulls
    expected_data = [("A", 1)]
    expected_df = spark.createDataFrame(expected_data, ["name", "value"])

    # Show the DataFrames for verification
    cleaned_df.show()
    expected_df.show()

    # Assert equality between the cleaned DataFrame and expected DataFrame
    assert_df_equality(cleaned_df, expected_df)


# Test case for enforcing DateType in a DataFrame
def test_date_type_enforcement(spark, data_prep):
    """Test enforcing DateType on date columns."""
    # Sample data with different date formats
    data = [("2023-12-01", "2023/12/02"), ("12/02/2023", "2023-12-03")]
    schema = StructType(
        [
            StructField("transaction_date", StringType(), True),
            StructField("alternate_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Desired schema to enforce
    desired_schema = StructType(
        [
            StructField("transaction_date", DateType(), True),
            StructField("alternate_date", DateType(), True),
        ]
    )

    # Apply data_enforce_type method to enforce the date schema
    result_df = data_prep.data_enforce_type(df, desired_schema)

    # Expected DataFrame with dates parsed correctly
    expected_data = [
        (
            spark.sql("SELECT to_date('2023-12-01', 'yyyy-MM-dd')").collect()[0][0],
            spark.sql("SELECT to_date('2023/12/02', 'yyyy/MM/dd')").collect()[0][0],
        ),
        (
            spark.sql("SELECT to_date('12/02/2023', 'MM/dd/yyyy')").collect()[0][0],
            spark.sql("SELECT to_date('2023-12-03', 'yyyy-MM-dd')").collect()[0][0],
        ),
    ]
    expected_df = spark.createDataFrame(expected_data, desired_schema)

    # Assert that the result matches the expected DataFrame
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


# Test case for enforcing IntegerType in a DataFrame
def test_integer_type_enforcement(spark, data_prep):
    """Test enforcing IntegerType on integer columns."""
    data = [("123",), ("456",), ("not_a_number",)]
    schema = StructType([StructField("int_col", StringType(), True)])
    df = create_test_df(spark, data, schema)

    # Expected DataFrame after enforcing IntegerType
    expected_data = [(123,), (456,), (None,)]
    expected_schema = StructType([StructField("int_col", IntegerType(), True)])
    expected_df = create_test_df(spark, expected_data, expected_schema)

    # Enforce IntegerType on the DataFrame
    enforced_df = data_prep.data_enforce_type(df, expected_schema)

    # Assert that the enforced DataFrame matches the expected DataFrame
    assert_df_equality(enforced_df, expected_df, ignore_row_order=True)


# Test case for enforcing FloatType in a DataFrame
def test_float_type_enforcement(spark, data_prep):
    """Test enforcing FloatType on float columns."""
    data = [("123.45",), ("456",), ("not_a_float",)]
    schema = StructType([StructField("float_col", StringType(), True)])
    df = create_test_df(spark, data, schema)

    # Expected DataFrame after enforcing FloatType
    expected_data = [(123.45,), (456.0,), (None,)]
    expected_schema = StructType([StructField("float_col", FloatType(), True)])
    expected_df = create_test_df(spark, expected_data, expected_schema)

    # Enforce FloatType on the DataFrame
    enforced_df = data_prep.data_enforce_type(df, expected_schema)

    # Assert that the enforced DataFrame matches the expected DataFrame
    assert_df_equality(enforced_df, expected_df, ignore_row_order=True)


# Test case for enforcing StringType in a DataFrame
def test_string_type_enforcement(spark, data_prep):
    """Test enforcing StringType on string columns."""
    data = [(1, "A"), (4, "B"), (None, "C")]
    schema = StructType(
        [
            StructField("int_col", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    df = create_test_df(spark, data, schema)

    # Expected DataFrame after enforcing StringType
    expected_data = [("1", "A"), ("4", "B"), (None, "C")]
    expected_schema = StructType(
        [
            StructField("int_col", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )
    expected_df = create_test_df(spark, expected_data, expected_schema)

    # Enforce StringType on the DataFrame
    enforced_df = data_prep.data_enforce_type(df, expected_schema)

    # Assert that the enforced DataFrame matches the expected DataFrame
    assert_df_equality(enforced_df, expected_df, ignore_row_order=True)


# Test case for handling missing columns
def test_missing_column(spark, data_prep):
    """Test behavior when a column to enforce is missing from the DataFrame."""
    data = [("2023-12-01",), ("2023-12-02",)]
    schema = StructType([StructField("date_col", StringType(), True)])
    df = create_test_df(spark, data, schema)

    # Call data_enforce_type with a non-existing column in the DataFrame
    enforced_df = data_prep.data_enforce_type(
        df, StructType([StructField("non_existing_date_col", StringType(), True)])
    )

    # Assert that the original DataFrame remains unchanged
    assert_df_equality(df, enforced_df, ignore_row_order=True)
