
import pandas as pd
import re
from pyspark.sql import SparkSession
from fuzzywuzzy import fuzz
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize SparkSession (if needed)
spark = SparkSession.builder.getOrCreate()

def clean_string(value):
    """Clean a string by removing non-alphabetic characters and converting to lower case."""
    if pd.isnull(value):
        return ""
    value = re.sub(r'[^a-zA-Z]', '', value)
    return value.lower().strip()

def clean_customer_name(customer_name):
    """Match the customer name against a list of defined customers."""
    defined_customers = [
        'A.U. Industries Dahej',
        'Unovel Industries',
        'Chamunda Plastics',
        'Parekh Plastics',
        'Global Coating',
        'Calco Poly Technik',
        'KPT Piping System',
        'A.U. Industries Ahm',
        'Parl Polytex',
        'Mor Techfab Ahm',
        'Mor Techfab Guj'
    ]
    
    best_match = None
    best_score = 0

    for defined_customer in defined_customers:
        score = fuzz.ratio(customer_name, defined_customer)
        if score > best_score:
            best_match = defined_customer
            best_score = score
    return best_match

def process_file(file_path):
    """Process the CSV file and return a cleaned DataFrame."""
    
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Get the file name
    file_name = file_path.rsplit('/', 1)[-1]
    
    # Clean the customer name from the file name
    cleaned_customer_name = clean_customer_name(file_name)

    # Rename columns to lower case
    df.columns = df.columns.str.lower()

    # Rename specified columns
    df.rename(columns={
        'vch_type': 'voucher_type', 
        'vch_no': 'voucher_number', 
        'debit': 'debit_amount_in_local_currency', 
        'credit': 'credit_amount_in_local_currency'
    }, inplace=True)

    # Find the last closing balance
    closing_balance_dict = df[df['particulars'].str.lower().str.strip() == "closing balance"]['particulars'].to_dict()

    if closing_balance_dict:
        last_index = list(closing_balance_dict.keys())[-1]
        debit_amount = df.loc[last_index, 'debit_amount_in_local_currency']
        credit_amount = df.loc[last_index, 'credit_amount_in_local_currency']
        last_closing_balance = debit_amount if pd.notnull(debit_amount) else credit_amount
    else:
        last_closing_balance = None

    # Drop rows where date, voucher number, and voucher type are null
    df.drop(df[(df['date'].isnull() | (df['date'] == '')) & df['voucher_number'].isnull() & df['voucher_type'].isnull()].index, inplace=True)

    # Forward fill date and convert to datetime
    df['date'] = df['date'].fillna(method='ffill')
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y', errors='coerce')
    
    # Filter dates
    df_filtered = df[df['date'] >= pd.to_datetime('01-04-2024', format='%d-%m-%Y')]
    df_filtered = df_filtered.rename(columns={'date': 'posting_date'})

    # Format the posting date
    df_filtered['posting_date'] = df_filtered['posting_date'].dt.strftime('%d-%m-%Y')

    # Clean customer names
    df_filtered['customer_name'] = df_filtered['customer_name'].apply(clean_customer_name)

    # Handle page breaks
    particulars_dict = df_filtered['particulars'].to_dict()
    voucher_number_dict = df_filtered['voucher_number'].to_dict()
    voucher_type_dict = df_filtered['voucher_type'].to_dict()

    rows_to_drop = []
    for idx in particulars_dict.keys():
        cleaned_particulars = clean_string(str(particulars_dict[idx]))
        cleaned_voucher_number = clean_string(str(voucher_number_dict.get(idx, "")))
        cleaned_voucher_type = clean_string(str(voucher_type_dict.get(idx, "")))

        if (cleaned_particulars == 'particulars' and 
            cleaned_voucher_number == 'vchno' and 
            cleaned_voucher_type == 'vchtype'):
            rows_to_drop.append(idx)

    df_filtered.drop(index=rows_to_drop, inplace=True)

    # Reset index of df_filtered
    df_filtered.reset_index(drop=True, inplace=True)

    # Create a new 'index' column for df_filtered
    df_filtered['index'] = range(1, len(df_filtered) + 1)

    # Calculate cumulative balance
    cumulative_balance = 0
    cumulative_balances = {}

    for i, row in df_filtered.iterrows():
        credit_amount = row['credit_amount_in_local_currency']
        debit_amount = row['debit_amount_in_local_currency']
        # Replace NaN values with 0
        credit_amount = credit_amount if pd.notnull(credit_amount) else 0
        debit_amount = debit_amount if pd.notnull(debit_amount) else 0

        cumulative_balance += credit_amount - debit_amount
        cumulative_balances[i] = cumulative_balance

    df_filtered['cumulative_amount_in_local_currency'] = df_filtered.index.map(cumulative_balances)

    # Check if the last cumulative value matches the last closing balance
    last_value = df_filtered['cumulative_amount_in_local_currency'].iloc[-1]  # last value of the cumulative balance column
    if last_value == last_closing_balance:
        print("The last value matches the last closing balance.")
    else:
        print("The last value does not match the last closing balance.")

    return df_filtered

def pandas_to_spark_silver_customer(silver_df_customer):
    # Initialize Spark session (if not already created)
    spark = SparkSession.builder \
        .appName("SilverCustomerConversion") \
        .getOrCreate()

    # Convert the pandas DataFrame to a Spark DataFrame
    spark_df1 = spark.createDataFrame(silver_df_customer)

    # Select the required columns
    required_columns = [
        "posting_date", "particulars", "voucher_type", "voucher_number", 
        "debit_amount_in_local_currency", "credit_amount_in_local_currency", 
        "customer_name", "cumulative_amount_in_local_currency"
    ]
    
    spark_df1 = spark_df1.select(*required_columns)

    # Cast columns to the correct types
    spark_silver_customer_df = (
        spark_df1
        .withColumn("posting_date", to_date(col("posting_date"), 'dd-MM-yyyy'))  # Keep as DATE
        .withColumn("particulars", col("particulars").cast("string"))
        .withColumn("voucher_type", col("voucher_type").cast("string"))
        .withColumn("voucher_number", col("voucher_number").cast("string"))
        .withColumn("debit_amount_in_local_currency", col("debit_amount_in_local_currency").cast("double"))
        .withColumn("credit_amount_in_local_currency", col("credit_amount_in_local_currency").cast("double"))
        .withColumn("customer_name", col("customer_name").cast("string"))
        .withColumn("cumulative_amount_in_local_currency", col("cumulative_amount_in_local_currency").cast("double"))
    )
    
    return spark_silver_customer_df
