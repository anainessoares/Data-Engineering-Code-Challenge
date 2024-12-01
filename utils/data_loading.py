import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class LoadingData:
    """
    This class handles data loading tasks using a SparkSession.
    It includes methods to load data from CSV files into DataFrames.
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Initializes the LoadingData class with a Spark session.

        :param spark: The SparkSession instance to be used for data loading.
        """
        self.spark_session = spark
        logging.info("STEP 1: Starting data loading")

    def read_csv(self, file_path: str, header: bool = True) -> DataFrame:
        """
        Reads a CSV file and loads it into a Spark DataFrame.

        :param file_path: Path to the CSV file to be read.
        :param header: Boolean indicating whether the CSV file has a header row. Defaults to True.
        :return: A Spark DataFrame containing the data from the CSV file.
        """
        logging.info(f"Reading dataset from {file_path}")

        try:
            # Load the CSV file into a DataFrame with optional header handling
            df = self.spark_session.read.csv(file_path, header=header)
            logging.info(f"Dataset successfully loaded from: {file_path}")
        except Exception as e:
            # Log any exception that occurs during the loading process
            logging.error(f"Failed to load data from {file_path}: {e}")
            raise  

        return df