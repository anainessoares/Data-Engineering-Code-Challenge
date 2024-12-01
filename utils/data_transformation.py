from pyspark.sql.types import StructType, StringType
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import logging


class TransformingData:
    """
    This class handles:
        - Data transformations
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Class initialization
        """
        self.spark_session = spark
        logging.info("STEP 3: Starting data transformations")

    def dim_sales_aggregation(self, df_1, df_2):
        """
        Sales Aggregation:
        - Calculate the total revenue for each store and each product category

        :param df_1: Input Spark DataFrame
        :param df_2: Input Spark DataFrame
        :return: New DataFrame with store_id, product_category and total_revenue
        """
        logging.info(
            "Step 3.1 - Building a new dimension with the total revenue for each store and each product category."
        )

        dim_sales_prod = df_1.join(df_2, on="product_id")

        dim_total_revenue = (
            dim_sales_prod.groupBy(
                F.col("store_id").alias("store_id"),
                F.col("category").alias("product_category"),
            )
            .agg(F.sum("price").alias("total_revenue"))
            .orderBy("store_id", "product_category")
        )

        dim_total_revenue = dim_total_revenue.select(
            "store_id", "product_category", "total_revenue"
        )

        # Save the store-level revenue insights as csv
        dim_total_revenue.write.csv(
            "output_data/dim_total_revenue", mode="overwrite", header=True
        )

        # Log results
        logging.info(
            "dim_total_revenue - New dimension with the total revenue for each store "
            "and each product category"
        )

        return dim_total_revenue

    def dim_total_quantity(self, df_1, df_2):
        """
        Monthly Sales Insights:
        - Calculate the total quantity sold for each product category, grouped by month.
        - Output: DataFrame with year, month, category, and total_quantity_sold.

        :param df_1: Input Spark DataFrame
        :param df_2: Input Spark DataFrame
        :return: New DataFrame with year, month, category, and total_quantity_sold
        """
        logging.info(
            "Step 3.2 - Building a new dimension with the total quantity sold "
            "for each product category, grouped by month."
        )

        df_1 = df_1.drop("update_timestamp")
        df_2 = df_2.drop("update_timestamp")

        dim_sales_prod = df_1.join(df_2, on="product_id")

        dim_sales_prod_dates = dim_sales_prod.withColumn(
            "month", F.date_format(F.col("transaction_date"), "MMMM")
        ).withColumn("year", F.year(F.col("transaction_date")))

        dim_total_qtt = (
            dim_sales_prod_dates.groupBy(
                F.col("category").alias("product_category"),
                F.col("month"),
                F.col("year"),
            )
            .agg(F.sum("quantity").alias("total_quantity"))
            .orderBy("category", "year", "month")
        )

        dim_total_qtt = dim_total_qtt.select(
            "year", "month", "product_category", "total_quantity"
        )

        # Log results
        logging.info(
            "dim_total_qtt - New dimension with the total quantity sold "
            "for each product category, grouped by month."
        )

        return dim_total_qtt

    def dim_enrich_data(self, df_1, df_2, df_3):
        """
        Enrich Data:
        - Combine the sales, products, and stores datasets into a single enriched dataset.
        - Columns: transaction_id, store_name, location, product_name, category,
          quantity, transaction_date, and price.

        :param df_1: Input Spark DataFrame
        :param df_2: Input Spark DataFrame
        :param df_3: Input Spark DataFrame
        :return: Enriched DataFrame
        """
        logging.info(
            "Step 3.3 - Building a new dimension with enriched data from multiple datasets."
        )

        df_1 = df_1.drop("update_timestamp")
        df_2 = df_2.drop("update_timestamp")
        df_3 = df_3.drop("update_timestamp")

        join_1 = df_1.join(df_2, on="product_id")
        enriched_df = join_1.join(df_3, on="store_id")

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

        # Save the DataFrame as Parquet, partitioned by category and transaction_date
        dim_enrich_data.write.partitionBy("category", "transaction_date").parquet(
            "output_data/parquet", mode="overwrite"
        )

        # Log results
        logging.info(
            "dim_enrich_data - New dimension with transaction_id, store_name, location, "
            "product_name, category, quantity, transaction_date, and price."
        )

        return dim_enrich_data

    def dim_price_category(self, df):
        """
        Price Categorization:
        - Categorize products into Low, Medium, and High based on price ranges.

        :param df: Input Spark DataFrame
        :return: DataFrame with a new 'price_category' column
        """

        # Categorization function
        def categorize_price(price):
            if price < 20:
                return "Low"
            elif 20 <= price <= 100:
                return "Medium"
            else:
                return "High"

        # UDF
        categorize_price_udf = F.udf(categorize_price, StringType())

        # Add price_category column
        df_with_category_price = df.withColumn(
            "price_category", categorize_price_udf(df["price"])
        )

        return df_with_category_price
