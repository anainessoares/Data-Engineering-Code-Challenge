import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType
from datetime import datetime


class TransformingData:
    """
    This class handles various data transformation tasks for Spark DataFrames.
    It includes methods for aggregating sales data, enriching data from multiple sources, and categorizing data.
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Initializes the TransformingData class with a Spark session.

        :param spark: The SparkSession instance.
        """
        self.spark_session = spark
        logging.info("STEP 3: Starting data transformations")

    def dim_sales_aggregation(self, df_1, df_2):
        """
        Aggregates sales data to calculate the total revenue for each store and product category.

        :param df_1: DataFrame containing sales data 
        :param df_2: DataFrame containing product data 
        :return: DataFrame with 'store_id', 'product_category', and 'total_revenue'
        """
        logging.info(
            "Step 3.1 - Building a new dimension with the total revenue for each store and each product category."
        )

        # Join sales data with product data on 'product_id'
        dim_sales_prod = df_1.join(df_2, on="product_id")

        # Aggregate total revenue by 'store_id' and product category
        dim_total_revenue = (
            dim_sales_prod.groupBy(
                F.col("store_id").alias("store_id"),
                F.col("category").alias("product_category"),
            )
            .agg(F.sum("price").alias("total_revenue"))
            .orderBy("store_id", "product_category")
        )

        # Select relevant columns for the final DataFrame
        dim_total_revenue = dim_total_revenue.select(
            "store_id", "product_category", "total_revenue"
        )

        # Save the aggregated data as CSV
        dim_total_revenue.write.csv(
            "output_data/dim_total_revenue", mode="overwrite", header=True
        )

        # Log the completion of the transformation
        logging.info(
            "dim_total_revenue - New dimension with the total revenue for each store "
            "and each product category"
        )

        return dim_total_revenue

    def dim_total_quantity(self, df_1, df_2):
        """
        Calculates total quantity sold for each product category, grouped by month.

        :param df_1: DataFrame containing sales data 
        :param df_2: DataFrame containing product data 
        :return: DataFrame with 'year', 'month', 'product_category', and 'total_quantity'
        """
        logging.info(
            "Step 3.2 - Building a new dimension with the total quantity sold for each product category, grouped by month."
        )

        # Join sales data with product data on 'product_id'
        dim_sales_prod = df_1.join(df_2, on="product_id")

        # Extract month and year from transaction_date
        dim_sales_prod_dates = dim_sales_prod.withColumn(
            "month", F.date_format(F.col("transaction_date"), "MMMM")
        ).withColumn("year", F.year(F.col("transaction_date")))

        # Group by month, year, and product category, and calculate total quantity sold
        dim_total_qtt = (
            dim_sales_prod_dates.groupBy(
                F.col("category").alias("product_category"),
                F.col("month"),
                F.col("year"),
            )
            .agg(F.sum("quantity").alias("total_quantity"))
            .orderBy("category", "year", "month")
        )

        # Select relevant columns for the final DataFrame
        dim_total_qtt = dim_total_qtt.select(
            "year", "month", "product_category", "total_quantity"
        )

        # Log the completion of the transformation
        logging.info(
            "dim_total_qtt - New dimension with the total quantity sold for each product category, grouped by month."
        )

        return dim_total_qtt

    def dim_enrich_data(self, df_1, df_2, df_3):
        """
        Enriches data by combining the sales, products, and stores datasets into a single enriched dataset.

        :param df_1: DataFrame containing sales data 
        :param df_2: DataFrame containing product data 
        :param df_3: DataFrame containing store data
        :return: Enriched DataFrame with combined data from all three inputs.
        """
        logging.info(
            "Step 3.3 - Building a new dimension with enriched data from multiple datasets."
        )

        # Join sales data with product data, then join the result with store data
        join_1 = df_1.join(df_2, on="product_id")
        enriched_df = join_1.join(df_3, on="store_id")

        # Select relevant columns for the final enriched DataFrame
        dim_enrich_data = enriched_df.select(
            "transaction_id",
            "store_name",
            "location",
            "product_name",
            "category",
            "quantity",
            "transaction_date",
            "price",
        )

        # Save the enriched data as a Parquet file, partitioned by category and transaction_date
        dim_enrich_data.write.partitionBy("category", "transaction_date").parquet(
            "output_data/parquet", mode="overwrite"
        )

        # Log the completion of the transformation
        logging.info(
            "dim_enrich_data - New dimension with transaction_id, store_name, location, "
            "product_name, category, quantity, transaction_date, and price."
        )

        return dim_enrich_data

    def dim_price_category(self, df):
        """
        Categorizes products into 'Low', 'Medium', and 'High' price ranges.

        :param df: DataFrame with a 'price' column to categorize.
        :return: DataFrame with a new 'price_category' column indicating the price range.
        """

        # Function to categorize price ranges
        def categorize_price(price):
            if price < 20:
                return "Low"
            elif 20 <= price <= 100:
                return "Medium"
            else:
                return "High"

        # Register the function as a UDF
        categorize_price_udf = F.udf(categorize_price, StringType())

        # Apply the UDF to create a new 'price_category' column
        df_with_category_price = df.withColumn(
            "price_category", categorize_price_udf(df["price"])
        )

        return df_with_category_price
