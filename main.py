import logging
from pyspark.sql import SparkSession
from utils.data_loading import LoadingData

# Logging configurations
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    logging.info("Initializing SparkSession.")
    spark = (
        SparkSession.builder.appName("DA Challenge")
        # .config("spark.sql.shuffle.partitions", "200")
        # .config("spark.executor.memory", "4g")
        # .config("spark.driver.memory", "2g")
        # .config("spark.dynamicAllocation.enabled", "true")
        # .config("spark.dynamicAllocation.maxExecutors", "50")
        .getOrCreate()
    )

    # Initialize LoadingData
    data_loader = LoadingData(spark)

    # Raw data paths
    sales_file = "raw_data/sales_uuid.csv"
    products_file = "raw_data/products_uuid.csv"
    stores_file = "raw_data/stores_uuid.csv"

    # Load datasets
    sales_df = data_loader.read_csv(sales_file)
    products_df = data_loader.read_csv(products_file)
    stores_df = data_loader.read_csv(stores_file)
