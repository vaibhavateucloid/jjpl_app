import os
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from sqlalchemy import create_engine, Table, MetaData, Column, String, Date, DECIMAL

from pyspark.sql import Window
from pyspark.sql.functions import col, coalesce, max, when, row_number, monotonically_increasing_id

def perform_reconciliation(df_customer_ledger, df_jjpl_ledger):
    spark = SparkSession.builder.getOrCreate()
    sqlite_db_path = 'ledger_reconciliation.db'
    conn = sqlite3.connect(sqlite_db_path)
    engine = create_engine(f'sqlite:///{sqlite_db_path}')

    # Create SQLite table if it doesn't exist
    with engine.connect() as conn_engine:
        if not engine.dialect.has_table(conn_engine, 'gold_jjpl_ledger_reconciliation'):
            metadata = MetaData()
            Table(
                'gold_jjpl_ledger_reconciliation', metadata,
                Column('customer_name', String),
                Column('posting_date', Date),
                Column('invoice_number', String),
                Column('amount_in_local_currency', DECIMAL(22, 5)),
                Column('flag', String)
            )
            metadata.create_all(engine)

    # Schema for gold output DataFrame
    gold_schema = StructType([
        StructField('customer_name', StringType(), True),
        StructField('posting_date', DateType(), True),
        StructField('invoice_number', StringType(), True),
        StructField('amount_in_local_currency', DoubleType(), True),
        StructField('flag', StringType(), True)
    ])

    df_gold = spark.createDataFrame([], gold_schema)

    # Get distinct customer names from both ledgers
    list_customer = [data[0] for data in df_jjpl_ledger.select('customer_name').distinct()
                     .join(df_customer_ledger.select('customer_name').distinct(),
                           df_jjpl_ledger['customer_name'].contains(df_customer_ledger['customer_name']), 'inner')
                     .collect()]

    # Add index to both DataFrames
    df_jjpl_ledger = df_jjpl_ledger.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))
    df_customer_ledger = df_customer_ledger.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))

    # Reconciliation logic
    window = Window.partitionBy('customer_name')

    for customer in list_customer:
        print('Customer: ', customer)

        # Subset data for current customer
        df_jjpl = df_jjpl_ledger.where(f"customer_name = '{customer}'") \
            .withColumn('max_index', max('index').over(window))
        df_customer = df_customer_ledger.where(f"customer_name = '{customer}'") \
            .withColumn('max_index', max('index').over(window))

        # Check if the closing balances match
        df_closing_balance_check = df_jjpl.alias('jjpl').select('cumulative_balance_lc', 'index', 'max_index') \
            .join(df_customer.alias('customer').select('cumulative_amount_in_local_currency', 'index', 'max_index'),
                  df_jjpl['cumulative_balance_lc'] == df_customer['cumulative_amount_in_local_currency'], 'inner')

        # Case 1: Closing balances match
        if df_closing_balance_check.where('jjpl.index = jjpl.max_index and customer.index = customer.max_index').collect():
            print('Closing Balance matches. Reconciliation complete')
            continue

        # Case 2: Additional records in JJPL ledger
        elif df_closing_balance_check.where('jjpl.index != jjpl.max_index and customer.index = customer.max_index').collect():
            print('Additional records in JJPL ledger')
            var_jjpl_index = df_closing_balance_check.where('customer.index = customer.max_index').select('jjpl.index').collect()[0][0]
            df_gold_temp = df_jjpl.where(f'index > {var_jjpl_index}') \
                .withColumn('invoice_number', col('origin_no')) \
                .withColumn('amount_in_local_currency', coalesce(col('debit_lc'), col('credit_lc'))) \
                .withColumn('flag', when(col('debit_lc').isNotNull(), lit('Dr by JJPL, but not Cr by Customer'))
                            .otherwise(lit('Cr by JJPL, but not Dr by Customer')))

        # Case 3: Additional records in Customer ledger
        elif df_closing_balance_check.where('jjpl.index = jjpl.max_index and customer.index != customer.max_index').collect():
            print('Additional records in Customer ledger')
            var_customer_index = df_closing_balance_check.where('jjpl.index = jjpl.max_index').select('customer.index').collect()[0][0]
            df_gold_temp = df_customer.where(f'index > {var_customer_index}') \
                .withColumn('invoice_number', col('voucher_number').cast('string')) \
                .withColumn('amount_in_local_currency', coalesce(col('debit_amount_in_local_currency'), col('credit_amount_in_local_currency'))) \
                .withColumn('flag', when(col('debit_amount_in_local_currency').isNotNull(), lit('Dr by Customer, but not Cr by JJPL'))
                            .otherwise(lit('Cr by Customer, but not Dr by JJPL')))

        # Case 4: Closing balances do not match
        else:
            print('Closing Balance does not match')
            var_customer_opening_balance = df_customer.where('posting_date = "2024-04-01" and lower(particulars) like "op%bal%"') \
                .select('cumulative_amount_in_local_currency').collect()[0][0]
            
            if df_jjpl.where(f'cumulative_balance_lc = {var_customer_opening_balance}').collect():
                print('Opening Balance matches')
                var_jjpl_last_match_index, var_customer_last_match_index = [index[0] for index in df_closing_balance_check
                    .where('jjpl.index != jjpl.max_index and customer.index != customer.max_index')
                    .agg(max(col('jjpl.index')).alias('jjpl_last_match_index'),
                         max(col('customer.index')).alias('customer_last_match_index')).collect()[0]]

                df_jjpl = df_jjpl.where(f'index > {var_jjpl_last_match_index}')
                df_customer = df_customer.where(f'index > {var_customer_last_match_index}')

                df_recon = df_jjpl.alias('jjpl').join(df_customer.alias('customer'),
                                                      (df_customer['voucher_number'].contains(df_jjpl['origin_no'])) |
                                                      ((df_customer['credit_amount_in_local_currency'] == df_jjpl['debit_lc']) |
                                                       (df_customer['debit_amount_in_local_currency'] == df_jjpl['credit_lc'])), 'full_outer')

                df_gold_temp = df_recon.withColumn('flag',
                    when(col('customer.index').isNull(),
                         when(col('debit_lc').isNull(), lit('Cr by JJPL, but not Dr by Customer'))
                         .otherwise(lit('Dr by JJPL, but not Cr by Customer')))
                    .when(col('jjpl.index').isNull(),
                          when(col('debit_amount_in_local_currency').isNull(), lit('Cr by Customer, but not Dr by JJPL'))
                          .otherwise(lit('Dr by Customer, but not Cr by JJPL')))) \
                    .where('flag is not null') \
                    .withColumn('customer_name', coalesce(col('jjpl.customer_name'), col('customer.customer_name'))) \
                    .withColumn('posting_date', coalesce(col('jjpl.posting_date'), col('customer.posting_date'))) \
                    .withColumn('amount_in_local_currency',
                                when(col('flag') == lit('Cr by JJPL, but not Dr by Customer'), col('credit_lc'))
                                .when(col('flag') == lit('Dr by JJPL, but not Cr by Customer'), col('debit_lc'))
                                .when(col('flag') == lit('Cr by Customer, but not Dr by JJPL'), col('credit_amount_in_local_currency'))
                                .otherwise(col('debit_amount_in_local_currency'))) \
                    .withColumn('flag', col('flag'))
            else:
                print('Closing Balance does not match and opening balance does not match')
                continue

        # Append the results to the gold DataFrame
        df_gold = df_gold.union(df_gold_temp.select(df_gold.columns))

    # Insert results into the SQLite table
    df_gold_pd = df_gold.toPandas()  # Convert to Pandas for SQLite insertion
    df_gold_pd.to_sql('gold_jjpl_ledger_reconciliation', conn, if_exists='append', index=False)

    print(df_gold_pd)

    # Close SQLite connection
    conn.close()
    
    return df_gold_pd
