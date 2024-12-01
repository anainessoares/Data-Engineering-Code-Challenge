import pytest
from pyspark.sql import SparkSession
from utils.data_loading import LoadingData
from chispa import assert_df_equality


@pytest.fixture(scope="module")
def spark():
    """
    Create and return a SparkSession for the tests
    """

    return SparkSession.builder.appName("TestsSession").getOrCreate()


@pytest.fixture
def data_loader(spark):
    """
    Create an instance of the LoadingData class
    """
    return LoadingData(spark)


def test_read_csv(data_loader, spark):
    """
    Test reading a CSV file into a DataFrame.
    """
    # Sample path for the CSV data
    test_products_csv_path = "tests/products.csv"

    # Loading the data using the data loader
    df_products = data_loader.read_csv(test_products_csv_path, header=True)

    # Expected DataFrame creation
    expected_products_data = [
        ('776efa5e-5fc1-499c-af9f-eb6ac465234c','Organic Apples','Fruit'),
        ('ed7d48cf-14df-4442-a59f-2b9b25afacc7','Almond Milk','Dairy'),
        ('06834baa-35cc-49d8-8379-bc4e2ccbd61a','Whole Wheat Bread','Bakery')
    ]
    expected_products_columns = ["product_id", "product_name", "category"]
    expected_df = spark.createDataFrame(expected_products_data, expected_products_columns)

    # Assert equality of the DataFrames
    assert_df_equality(df_products, expected_df, ignore_row_order=True)

