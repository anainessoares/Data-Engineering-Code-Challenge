import logging
from pyspark.sql import SparkSession, functions as F
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
    This class handles data preparations and validations for Spark DataFrames.
    It includes methods for handling null values, removing duplicates, and enforcing data types.
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialize the PreparationValidationData class with a Spark session.

        :param spark: The SparkSession instance.
        """
        self.spark_session = spark
        logging.info("STEP 2: Starting data preparations and validations")

    def data_handle_nulls(self, df, strategy: str = "drop", subset: list = None):
        """
        Handle null values in a DataFrame based on the specified strategy.

        :param df: Input Spark DataFrame.
        :param strategy: Strategy to handle nulls. Currently, only "drop" is supported.
        :param subset: List of columns to check for nulls; checks all columns if None.
        :return: A DataFrame with nulls handled according to the strategy.
        """
        logging.info("Step 2.1 - Null values verification started.")

        # If subset is not provided, check all columns
        subset = subset or df.columns

        # Count the total number of rows before cleaning
        total_rows = df.count()
        logging.info(f"Total rows before handling nulls: {total_rows}")

        # Handle nulls based on the provided strategy
        if strategy == "drop":
            logging.info("Dropping rows with null values.")
            df = df.dropna(how="any", subset=subset)
        else:
            raise ValueError(f"Unsupported strategy '{strategy}'. Use 'drop'.")

        # Log the number of rows after cleaning
        cleaned_rows = df.count()
        logging.info(f"Total rows after cleaning: {cleaned_rows}")
        logging.info(f"Rows removed: {total_rows - cleaned_rows}")

        return df

    def data_drop_duplicates(self, df, subset: list = None, action: str = "drop"):
        """
        Remove duplicate rows from a DataFrame.

        :param df: Input DataFrame.
        :param subset: Columns to check for duplicates. Checks all columns if None.
        :param action: Action to take on duplicates. Only "drop" is supported.
        :return: A DataFrame with duplicates handled according to the action.
        """
        logging.info("Step 2.2 - Checking for duplicate rows.")

        # If subset is not provided, check the entire row for duplicates
        subset = subset or df.columns

        # Count initial number of rows and the number of distinct rows
        initial_count = df.count()
        distinct_count = df.dropDuplicates(subset=subset).count()
        duplicate_count = initial_count - distinct_count

        logging.info(f"Duplicate rows: {duplicate_count} based on columns: {subset}")

        # Drop duplicates if the action is 'drop'
        if action == "drop":
            logging.info("Dropping duplicate rows.")
            df = df.dropDuplicates(subset=subset)
        else:
            raise ValueError(f"Unsupported action: '{action}'. Use 'drop'.")

        # Log the number of rows after removing duplicates
        cleaned_count = df.count()
        logging.info(f"Total rows after handling duplicates: {cleaned_count}")

        return df

    def data_enforce_type(self, df, schema: dict):
        """
        Enforce data types on columns in a DataFrame with error handling.

        :param df: Input DataFrame.
        :param schema: A dictionary of column names and their desired data types.
        :return: DataFrame with enforced schema.
        """
        logging.info("Step 2.3 - Enforcing data schema.")

        # Create a dictionary from the schema for easy type lookups
        schema_dict = {field.name: field.dataType for field in schema}

        # Iterate through each column to enforce its data type
        for col_name, desired_dtype in schema_dict.items():
            if col_name not in df.columns:
                logging.warning(f"Column '{col_name}' not found, skipping.")
                continue

            # Handle DateType
            if str(desired_dtype) == "DateType()":
                # Define acceptable date formats
                date_formats = [
                    "yyyy-MM-dd",
                    "MM/dd/yyyy",
                    "yyyy/MM/dd",
                    "dd-MM-yyyy",
                    "MMMM dd, yyyy",
                ]

                # Convert the column to DateType using coalesce for multiple formats
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
                    f"Data type '{desired_dtype}' for column '{col_name}' is not handled, skipping."
                )

        return df
