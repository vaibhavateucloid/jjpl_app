import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType
from pyspark.sql.functions import col, to_date
from customer_bronze_to_silver import *

def clean_ledger_data(filename):
    # Read the Excel file with the second row (header=1) as the header
    sdf = pd.read_excel(filename, header=1)

    # List of columns to extract
    columns_1 = [
        "Posting Date", "Due Date", "Trans. No.", "Offset Account", "Origin", "Origin No.",
        "Ref. 2", "Details", "Debit (LC)", "Credit (LC)", "Cumulative Balance (LC)",
        "Balance Due (LC)", "Created By", "Branch"
    ]

    # Corresponding new column names
    columns_2 = [
        "Posting_Date", "Due_Date", "Trans_No", "Offset_Account", "Origin", "Origin_No",
        "Ref_2", "Details", "Debit_LC", "Credit_LC", "Cumulative_Balance_LC",
        "Balance_Due_LC", "Created_By", "Branch"
    ]

    # Extract only the columns of interest
    sdf_clean = sdf[columns_1]

    # Rename columns
    sdf_clean = sdf_clean.rename(columns=dict(zip(columns_1, columns_2)))

    # Extract the filename without the extension
    full_filename_without_extension, _, extension = filename.rpartition(".")
    filename_without_extension = full_filename_without_extension.split("/")[-1]

    # Apply the clean_customer_name function to the extracted customer name from the filename
    cleaned_customer_name = clean_customer_name(filename_without_extension)

    # Add the cleaned customer_name column
    sdf_clean['customer_name'] = cleaned_customer_name

    # Add an index column
    sdf_clean['index'] = sdf_clean.index + 1

    # Convert 'Ref_2' to string
    sdf_clean['Ref_2'] = sdf_clean['Ref_2'].astype(str)

    # Convert date columns
    sdf_clean["Posting_Date"] = pd.to_datetime(sdf_clean["Posting_Date"], errors='coerce').dt.date
    sdf_clean["Due_Date"] = pd.to_datetime(sdf_clean["Due_Date"], errors='coerce').dt.date

    # Handle NaT values by replacing with None
    sdf_clean["Posting_Date"] = sdf_clean["Posting_Date"].where(sdf_clean["Posting_Date"].notna(), None)
    sdf_clean["Due_Date"] = sdf_clean["Due_Date"].where(sdf_clean["Due_Date"].notna(), None)

    # Convert monetary columns
    monetary_columns = ["Debit_LC", "Credit_LC", "Cumulative_Balance_LC", "Balance_Due_LC"]
    for col in monetary_columns:
        sdf_clean[col] = pd.to_numeric(sdf_clean[col].replace('[^0-9.-]', '', regex=True), errors='coerce')

    # Reset the index and reorder the columns
    sdf_clean.reset_index(drop=True, inplace=True)
    sdf_clean['index'] = sdf_clean.index + 1

    # Define the new column order
    column_order = ['index'] + columns_2 + ['customer_name']
    sdf_clean = sdf_clean[column_order]

    # Convert all column names to lowercase
    sdf_clean.columns = sdf_clean.columns.str.lower()

    # Convert NaN values to None
    # sdf_clean[['credit_lc', 'debit_lc']] = sdf_clean[['credit_lc', 'debit_lc']].fillna(value=None)

    print("Data cleaned and saved!")
    os.remove(filename)
    return sdf_clean

def pandas_to_spark_cleaned(sdf_cleaned):
    print("Data types in Panas DataFrame before conversion:")
    print(sdf_cleaned.dtypes)
    
    # Initialize Spark session (if not already created)
    spark = SparkSession.builder \
        .appName("CleanedLedgerConversion") \
        .getOrCreate()

    schema = StructType([
        StructField("index", IntegerType(), True),
        StructField("posting_date", DateType(), True),
        StructField("due_date", DateType(), True),
        StructField("trans_no", FloatType(), True),
        StructField("offset_account", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("origin_no", FloatType(), True),
        StructField("ref_2", StringType(), True),
        StructField("details", StringType(), True),
        StructField("debit_lc", FloatType(), True),
        StructField("credit_lc", FloatType(), True),
        StructField("cumulative_balance_lc", FloatType(), True),
        StructField("balance_due_lc", FloatType(), True),
        StructField("created_by", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])

    spark_df = spark.createDataFrame(sdf_cleaned, schema=schema)

    # Cast columns to the correct types (if needed)
    spark_df = spark_df.withColumn("index", col("index").cast("int")) \
                       .withColumn("posting_date", to_date(col("posting_date"))) \
                       .withColumn("due_date", to_date(col("due_date"))) \
                       .withColumn("trans_no", col("trans_no").cast("double")) \
                       .withColumn("offset_account", col("offset_account").cast("string")) \
                       .withColumn("origin", col("origin").cast("string")) \
                       .withColumn("origin_no", col("origin_no").cast("double")) \
                       .withColumn("ref_2", col("ref_2").cast("string")) \
                       .withColumn("details", col("details").cast("string")) \
                       .withColumn("debit_lc", col("debit_lc").cast("double")) \
                       .withColumn("credit_lc", col("credit_lc").cast("double")) \
                       .withColumn("cumulative_balance_lc", col("cumulative_balance_lc").cast("double")) \
                       .withColumn("balance_due_lc", col("balance_due_lc").cast("double")) \
                       .withColumn("created_by", col("created_by").cast("string")) \
                       .withColumn("branch", col("branch").cast("string")) \
                       .withColumn("customer_name", col("customer_name").cast("string"))

    return spark_df
