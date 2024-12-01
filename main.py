import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)
from utils.data_loading import LoadingData
from utils.data_preparation import PreparationValidationData
from utils.data_transformation import TransformingData

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

    ########### Initialize LoadingData
    data_loader = LoadingData(spark)

    # Raw data paths
    sales_file = "raw_data/sales_uuid.csv"
    products_file = "raw_data/products_uuid.csv"
    stores_file = "raw_data/stores_uuid.csv"

    # Load datasets
    sales_df = data_loader.read_csv(sales_file)
    products_df = data_loader.read_csv(products_file)
    stores_df = data_loader.read_csv(stores_file)

    ############ Preparations and validations of datasets
    data_prep = PreparationValidationData(spark)

    # Handle nulls
    nulls_sales_df = data_prep.data_handle_nulls(sales_df, strategy="drop")

    nulls_products_df = data_prep.data_handle_nulls(products_df, strategy="drop")

    nulls_stores_df = data_prep.data_handle_nulls(stores_df, strategy="drop")

    # Handle duplicates
    dup_sales_df = data_prep.data_drop_duplicates(
        nulls_sales_df, subset=["transaction_id"]
    )
    dup_products_df = data_prep.data_drop_duplicates(
        nulls_products_df, subset=["product_id"]
    )
    dup_stores_df = data_prep.data_drop_duplicates(nulls_stores_df, subset=["store_id"])

    # Enforce data types
    sales_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("price", FloatType(), True),
        ]
    )
    dim_sales_df = data_prep.data_enforce_type(dup_sales_df, schema=sales_schema)

    products_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )
    dim_products_df = data_prep.data_enforce_type(
        dup_products_df, schema=products_schema
    )

    stores_schema = StructType(
        [
            StructField("store_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
        ]
    )
    dim_stores_df = data_prep.data_enforce_type(dup_stores_df, schema=stores_schema)

    ########### Transformations
    # Initialize Transformations
    # Create an instance of TransformingData
    data_transformer = TransformingData(spark)


    dim_sales_aggregation = data_transformer.dim_sales_aggregation(
        dim_sales_df, dim_products_df
    )

    dim_total_quantity = data_transformer.dim_total_quantity(
        dim_sales_df, dim_products_df
    )

    dim_enrich_data = data_transformer.dim_enrich_data(
        dim_sales_df, dim_products_df, dim_stores_df
    )

    dim_price_category = data_transformer.dim_price_category(dim_enrich_data)

    logging.info("Data pipeline completed successfully.")


if __name__ == "__main__":
    main()
