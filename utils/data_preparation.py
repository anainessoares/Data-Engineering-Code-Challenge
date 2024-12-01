from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, functions as F
import logging
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)


class PreparationValidationData:
    """
    This class handles:
        - Data preparations and validations
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Class initialization
        """
        self.spark_session = spark
        logging.info("STEP 2: Starting data preparetions and validations")

    def data_handle_nulls(self, df, strategy: str = "drop", subset: list = None):
        """
        Clean the input DataFrame by handling null values.

        :param df: Input Spark DataFrame
        :param strategy: Strategy to handle nulls:
            - "drop_values": Drop rows with nulls
        :param subset: List of columns to check for nulls; checks all columns if None
        :return: Cleaned DataFrame
        """
        logging.info("Step 2.1 - Null values verification started.")

        # Select columns to check
        subset = subset or df.columns

        # Total number of rows
        total_rows = df.count()
        logging.info(f"Total rows before handle_nulls: {total_rows}")

        # Handle nulls based on strategy
        if strategy == "drop":
            logging.info("Dropping rows with null values")
            df = df.dropna(how="any", subset=subset)
        else:
            raise ValueError(f"Unsupported strategy '{strategy}'. Use 'drop'")

        # Log results
        cleaned_rows = df.count()
        logging.info(f"Total rows after cleaning: {cleaned_rows}")
        logging.info(f"Rows removed or modified: {total_rows - cleaned_rows}")

        return df

    def data_drop_duplicates(self, df, subset: list = None, action: str = "drop"):
        """
        Handle duplicate rows in a DataFrame.

        :param df: Input DataFrame
        :param subset: List of columns to check for duplicates, if none, checks the entire row
        :param action: Action to take for duplicates:
            - "drop": Remove duplicate rows
        :return: DataFrame with duplicates handled based on the action.
        """
        logging.info("Step 2.2 - Checking for duplicate rows.")
        subset = subset or df.columns
        initial_count = df.count()
        distinct_count = df.dropDuplicates(subset=subset).count()
        duplicate_count = initial_count - distinct_count

        logging.info(f"Duplicate rows: {duplicate_count} based on {subset}")
        if action == "drop":
            logging.info("Dropping duplicate rows.")
            df = df.dropDuplicates(subset=subset)
        else:
            raise ValueError(f"Unsupported action: {action}")

        cleaned_count = df.count()
        logging.info(f"Total rows after handling duplicates: {cleaned_count}")
        return df

    def data_enforce_type(self, df, schema: dict):
        """
        Enforce column data types on a DataFrame with validation and error handling.

        :param df: Input DataFrame.
        :param schema: Dictionary of column names and their desired data types
        :return: DataFrame with enforced schema and errors handled based on the specified option.
        """
        logging.info("Step 2.3 - Enforcing data schema.")
        schema_dict = {field.name: field.dataType for field in schema}

        for col_name, desired_dtype in schema_dict.items():
            if col_name not in df.columns:
                logging.warning(f"Column '{col_name}' not found, skipping.")
                continue

            # Handle DateType
            if str(desired_dtype) == "DateType()":
                date_formats = [
                    "yyyy-MM-dd",
                    "MM/dd/yyyy",
                    "yyyy/MM/dd",
                    "dd-MM-yyyy",
                    "MMMM dd, yyyy",
                ]

                df = df.withColumn(
                    col_name,
                    F.coalesce(
                        *[F.to_date(F.col(col_name), fmt) for fmt in date_formats]
                    ),
                )
            # Handle IntegerType
            elif isinstance(desired_dtype, IntegerType):
                df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

            # Handle FloatType
            elif isinstance(desired_dtype, FloatType):
                df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))

            # Handle StringType
            elif isinstance(desired_dtype, StringType):
                df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
            else:
                logging.warning(
                    f"Column '{col_name}' casting not defined in the code, skipping."
                )
                continue

        return df
