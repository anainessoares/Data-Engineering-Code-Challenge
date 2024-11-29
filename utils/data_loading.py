import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class LoadingData:
    """
    This class handles:
        - Data loading with a SparkSession
        - Loading csv files as a DataFrame
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Class initialization.
        """
        self.spark_session = spark
        logging.info("STEP 1: Starting data loading")

    def read_csv(self, file_path: str, header: bool = True) -> DataFrame:
        """
        Read a csv file into a Spark DataFrame.

        :param file_path: Path to the csv file.
        :param header: Whether the csv has a header.
        :return: Spark DataFrame.
        """
        logging.info(f"Reading dataset from {file_path}")

        try:
            # Load datasets
            df = self.spark_session.read.csv(file_path, header=header)
            logging.info(f"Loaded dataset in: {file_path}")
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise

        return df
