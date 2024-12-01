# Data Processing Pipeline Documentation

## Project Overview
This project implements a PySpark-based data pipeline designed to load, prepare, validate, and transform data efficiently. 
The pipeline handles input data in CSV format, performs various data cleaning and transformation tasks, and outputs structured and meaningful data for further analysis.

## Approach to the Problem
The data pipeline is modularized into distinct stages, each represented by a class:

1. **LoadingData Class**: Responsible for loading raw data from CSV files into PySpark DataFrames.
2. **PreparationValidationData Class**: Handles data preparation and validation tasks, such as null handling, duplicate removal, and data type enforcement.
3. **TransformingData Class**: Implements data transformation logic for aggregating sales data, calculating quantities sold, enriching data from multiple sources, and categorising products based on their prices.

## Detailed Steps

### 1. Loading Data
- Data is loaded from CSV files using the `read_csv` method of the `LoadingData` class.
- The data is stored in PySpark DataFrames for further processing.

### 2. Data Preparation and Validation
- **Null Handling**: The `data_handle_nulls` method drops rows with null values based on the specified strategy.
- **Duplicate Handling**: The `data_drop_duplicates` method removes duplicates or flags them for review.
- **Data Type Enforcement**: The `data_enforce_type` method ensures that columns conform to the defined schema.

### 3. Data Transformation
- **Sales Aggregation**: The `dim_sales_aggregation` method calculates total revenue for each store and product category.
- **Total Quantity Calculation**: The `dim_total_quantity` method computes total quantity sold for each product category, grouped by month.
- **Data Enrichment**: The `dim_enrich_data` method combines sales, products, and store information into an enriched dataset.
- **Price Categorization**: The `dim_price_category` method classifies products into price categories.

## Assumptions and Decisions

### Assumptions
- **Environment**: The code is assumed to be executed within a PySpark environment with access to necessary libraries.
- **Data Format**: The CSV files used as input are formatted correctly with headers matching the expected structure.
- **Spark Configuration**: A SparkSession instance must be initialised for the pipeline to run.

### Decisions
- **Modular Design**: The code is structured into separate classes and methods to enhance readability, maintainability, and reusability.
- **Error Handling**: Basic error handling is incorporated to ensure that unexpected data issues are caught during processing.
- **Logging**: Logging is used throughout the code to track the execution flow and capture any issues for easier debugging.
- **Partitioning Output**: Output data is partitioned where necessary for better performance during data storage and retrieval.

## Steps to Run the Code and Reproduce Results

### Prerequisites
- Ensure you have installed the required libraries by running:
  ```bash
  pip install -r requirements.txt
  ```

### Running the Code
1. **Prepare the data**: Ensure that the raw data files (`sales_uuid.csv`, `products_uuid.csv`, `stores_uuid.csv`) are available in the `raw_data` directory.
2. **Run the main script**: The main script, located in `main.py`, orchestrates the entire pipeline. Run it using:
   ```bash
   python main.py
   ```
3. **Testing**: To test the individual classes and methods, run `pytest` on the test files located in the `tests` directory:
   ```bash
   pytest tests/
   ```

### Expected Output
- The output data is stored in the `output_data` directory. The transformed data is saved in various formats such as CSV and Parquet.
- Logs generated during execution can be found in the console or in a specified log file for debugging purposes.

## File Structure
```plaintext
project_root/
|-- main.py
|-- utils/
    |-- __init__.py
|   |-- data_loading.py
|   |-- data_preparation.py
|   |-- data_transformation.py
|-- raw_data/
|   |-- sales_uuid.csv
|   |-- products_uuid.csv
|   |-- stores_uuid.csv
|-- output_data/
|-- tests/
    |-- __init__.py
|   |-- test_data_loading.py
|   |-- test_data_preparation.py
|-- requirements.txt
```