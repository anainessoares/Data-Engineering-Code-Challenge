# Data Engineering Code Challenge
Data Engineering Code Challenge (PySpark)

## Objective:
The purpose of this challenge is to evaluate your skills in data engineering, particularly in working with PySpark for data processing and transformation tasks. This challenge mimics real-world scenarios you might encounter in the role.

### Technical information:
- Use **Python** and **PySpark** [https://spark.apache.org/docs/latest/api/python/getting_started/install.html]
- Use the following package for PySpark tests - https://github.com/MrPowers/chispa - the application **needs** to have tests.
- **Do not use** notebooks for final delivery, like **Jupyter** for instance. While these are good for interactive work and/or prototyping in this case they shouldn't be used.
- The application should have an automated build pipeline using GitHub Actions.
- Follow best practices by using descriptive commit messages.
- Use **logging** to show information about what the application is doing, avoid using **print** statements, but feel free to use them in the tests for your own debugging purposes. However, they should not be there in the final version.
- Use type hints and docstrings as much as possible to enhance code documentation and readability.
- Consider using linters and code formatters and have it the Continuous Integration step of your automated build.

## Background:
You work for a company that analyzes retail sales data from various stores. Your task is to process raw transactional data, generate insights, and save the transformed data for further analysis.

The raw data consists of:
- A **Sales Transactions** dataset with information about products sold, quantities, and prices.
- A **Products** dataset containing product details.
- A **Stores** dataset with details about stores.

### Data Description
1. **Sales Transactions** Dataset (sales.csv):
    - ```transaction_id```: Unique identifier for the transaction.
    - ```store_id```: ID of the store where the transaction occurred.
    - ```product_id```: ID of the product sold.
    - ```quantity```: Number of units sold.
    - ```transaction_date```: Date of the transaction (format: YYYY-MM-DD).
    - ```price```: Price per unit.
2. **Products** Dataset (products.csv):
    - ```product_id```: Unique identifier for the product.
    - ```product_name```: Name of the product.
    - ```category```: Category of the product.
3. **Stores** Dataset (stores.csv):
    - ```store_id```: Unique identifier for the store.
    - ```store_name```: Name of the store.
    - ```location```: Location of the store.

## Tasks
You need to perform the following tasks using PySpark:
1. Part 1: **Data Preparation**
- Load the three datasets (sales.csv, products.csv, and stores.csv) into PySpark DataFrames.
- Perform basic data validation:
    - Check for missing or null values as well as inconsistencies in the data format.
    - Identify and handle duplicates, if any.
    - Enforce the appropriate data types for all columns.

2. Part 2: **Data Transformation**

    **Sales Aggregation**:
    - Calculate the total revenue for each store (store_id) and each product category.
    - Output: DataFrame with store_id, category, and total_revenue.

    **Monthly Sales Insights**:
    - Calculate the total quantity sold for each product category, grouped by month.
    - Output: DataFrame with year, month, category, and total_quantity_sold.

    **Enrich Data**:
    - Combine the sales, products, and stores datasets into a single enriched dataset with the following columns:
```transaction_id```, ```store_name```, ```location```, ```product_name```, ```category```, ``quantity``, ``transaction_date``, and ```price```.

    **PySpark UDF**:
    - Implement a PySpark UDF to categorize products based on the following price ranges:

        ```
        Low: price < 20
        Medium: 20 <= price <= 100
        High: price > 100
        ```

    - Add a column price_category to the enriched dataset and save it as an additional output.



3. Part 3: **Data Export**
    - Save the enriched dataset from Part 2, Task 3, in Parquet format, partitioned by category and transaction_date.
    - Save the store_id-level revenue insights (from Part 2, Task 1) in CSV format.

    
## Deliverables
- Your solution should be uploaded and submitted using this repo as base (download, clone, for is not allowed)
- Your solution should follow good project architecture.

- **Output files**:
    - Enriched dataset in Parquet format.
    - Revenue insights in CSV format.
    - Create a documentation file in Markdown for documenting your project including:
        - Your approach to the problem.
        - Any assumptions or decisions made.
        - Steps to run the code and reproduce results.

- **Submission Guidelines**
    - Once you finish the challenge ping us via the contact mail we provided. 

## Evaluation Criteria
- Correctness and completeness of the solution.
- Efficient use of PySpark for data processing.
- Code quality (readability, comments, modularity, good practices).
- Adherence to the deliverable format and instructions.
- Handling of edge cases and data validation.





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