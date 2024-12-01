import pytest
from pyspark.sql import SparkSession
from utils.data_loading import LoadingData
from chispa import assert_df_equality


@pytest.fixture(scope="module")
def spark():
    """
    Create and return a SparkSession for the tests
    """

    return SparkSession.builder.appName("TestSession").getOrCreate()


@pytest.fixture
def data_loader(spark):
    """
    Create an instance of the LoadingData class
    """
    return LoadingData(spark)


def test_read_csv(data_loader, spark):
    """
    Test reading a CSV file into a DataFrame
    """

    # Sample path for the CSV data
    test_products_csv_path = "tests/products.csv"

    # Loading the data
    df_products = data_loader.read_csv(test_products_csv_path, header=True)

    # DataFrame is not empty
    assert df_products.count() > 0, "Products DataFrame should not be empty."

    # Expected DataFrame
    expected_products_columns = ["product_id", "product_name", "category"]
    assert (
        df_products.columns == expected_products_columns
    ), "Products DataFrame schema mismatch."

    # Create an expected DataFrame for comparison
    expected_data = [
        ("ff93357b-d490-4dc5-a773-7183a0c3e199", "Chicken", "Meat"),
        ("ff93357b-d490-4dc5-a773-7183a0c3e100", "Olive Oil", "Condiments"),
    ]
    expected_df = spark.createDataFrame(expected_data, expected_products_columns)
    assert_df_equality(df_products, expected_df, ignore_row_order=True)
