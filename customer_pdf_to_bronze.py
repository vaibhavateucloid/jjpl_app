# Converts one pdf to respective csv that we can ingest in the bronze layer

import os
import boto3
import csv
import io
from dotenv import load_dotenv
from pdf2image import convert_from_path
import shutil
import pandas as pd
import csv
from io import StringIO
import re
from datetime import datetime
import cv2
import numpy as np
from PIL import Image
# import pandas as pd
# import re
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
import pandas as pd
# from fuzzywuzzy import fuzz
from customer_bronze_to_silver import *


# def preprocess_image(image_path):
#     # Read the image
#     image = cv2.imread(image_path)
    
#     # Convert to grayscale
#     gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
#     # Apply thresholding to preprocess the image
#     gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    
#     # Apply dilation and erosion to remove some noise
#     kernel = np.ones((1, 1), np.uint8)
#     gray = cv2.dilate(gray, kernel, iterations=1)
#     gray = cv2.erode(gray, kernel, iterations=1)
    
#     # Apply median blur to remove noise
#     processed_image = cv2.medianBlur(gray, 3)
    
#     # Save the preprocessed image
#     cv2.imwrite(image_path, processed_image)
    
#     return image_path

def pdf_to_pngs(pdf_file, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    pages = convert_from_path(pdf_file, 500)  # 300 DPI
    page_files = []
    
    for i, page in enumerate(pages):
        page_file = os.path.join(output_folder, f'page_{i + 1}.png')
        page.save(page_file, 'PNG')
        page_files.append(page_file)
    
    return page_files

def print_table_headers(table_csv):
    # Use StringIO to read the CSV data from a string
    csv_data = StringIO(table_csv)
    
    # Use csv.reader to parse the CSV data
    reader = csv.reader(csv_data)
    
    # Extract the headers (first row)
    headers = next(reader)
    
    # Print the headers
    print("Headers:", headers)

def clean_table_csv(table_csv):
    # Use StringIO to read the CSV data from a string
    csv_data = StringIO(table_csv)
    reader = csv.reader(csv_data)

    # Extract the headers (first row)
    headers = next(reader)

    # Filter out any headers that are empty or just whitespace
    cleaned_headers = [header for header in headers if header.strip()]

    # Store the cleaned rows, excluding empty columns
    cleaned_rows = []
    
    for row in reader:
        # Create a new row excluding values from unnamed columns (based on cleaned headers)
        cleaned_row = [value for i, value in enumerate(row) if headers[i].strip()]
        cleaned_rows.append(cleaned_row)

    # Create output CSV with cleaned data
    output_csv = StringIO()
    writer = csv.writer(output_csv)

    # Write the cleaned headers and rows to the output CSV
    writer.writerow(cleaned_headers)
    writer.writerows(cleaned_rows)

    # Return the cleaned CSV data as a string
    return output_csv.getvalue()

def cleanup_csv(file_path):
    customer_name = os.path.splitext(os.path.basename(file_path))[0]
    
    df = pd.read_csv(file_path)
    
    df.columns = df.columns.str.strip()

    columns_1 = ['Date', 'Particulars', 'Vch Type', 'Vch No.', 'Debit', 'Credit']
    columns_2 = ['date', 'particulars', 'vch_type', 'vch_no', 'debit', 'credit']

    df_clean = df[columns_1]
    
    df_clean = df_clean.rename(columns=dict(zip(columns_1, columns_2)))

    # Clean up 'particulars' by removing 'Dr' and 'Cr', and extra spaces
    df_clean['date'] = df_clean['date'].str.replace(r'(Dr|Cr|By|To)', '', regex=True)  # Remove 'Dr' or 'Cr'
    df_clean['date'] = df_clean['date'].str.replace(r'\s+', ' ', regex=True)  # Replace multiple spaces with a single space
    df_clean['date'] = df_clean['date'].str.replace(r'(\d{1,2})-(\w)\s*(\w+)-(\d{2})', r'\1-\2\3-\4', regex=True)  # Handle space between letters
    df_clean['date'] = df_clean['date'].str.strip()  # Remove leading and trailing whitespaces
    # Function to handle different date formats

    def parse_date(date_val):
        # this function fails for '13-Aug-24 Agst' type of date so it is actually '13-Aug-24' so check if the first split on whitespace is a date
        if pd.isna(date_val):
            return pd.NaT
        
        if ' ' in date_val:
            date_val = date_val.split()[0]

        try:
            date = datetime.strptime(date_val, "%d-%b-%y")
            return date.strftime("%d-%m-%Y")
        except ValueError:
            pass
        
        # Try "d-m-yyyy" format (Parekh Plastics)
        try:
            date = datetime.strptime(date_val, "%d-%m-%Y")
            return date.strftime("%d-%m-%Y")
        except ValueError:
            pass
        
        # If both fail, return NaT
        return pd.NaT

    # def parse_date(date_val):
    #     # Handle NaN values
    #     if pd.isna(date_val):
    #         return pd.NaT

    #     # First step: Use regex to match the potential date parts in the string
    #     # We allow for the month to be split into two parts (e.g., "J an")
    #     date_pattern = re.search(r'(\d{1,2})-([A-Za-z]+(?:\s+[A-Za-z]+)*)-(\d{2})', date_val)

    #     if date_pattern:
    #         # Extract the date components (day, month, year)
    #         day = date_pattern.group(1).replace(" ", "")
    #         month = date_pattern.group(2).replace(" ", "")  # Remove spaces in month part (e.g., 'J an' -> 'Jan')
    #         year = date_pattern.group(3).replace(" ", "")

    #         # Reconstruct the cleaned date string
    #         cleaned_date = f"{day}-{month}-{year}"

    #         try:
    #             # Try parsing the cleaned date string in "%d-%b-%y" format (e.g., "13-Aug-24")
    #             date = datetime.strptime(cleaned_date, "%d-%b-%y")
    #             return date.strftime("%d-%m-%Y")
    #         except ValueError:
    #             pass

    #     # Return NaT if no valid date is found or parsing fails
    #     return pd.NaT

    # Clean up the 'date' column
    df_clean['date'] = df_clean['date'].apply(parse_date)

    # Clean the monetary columns ('debit' and 'credit') by removing any non-numeric characters and converting to numeric
    monetary_columns = ["debit", "credit"]
    for col in monetary_columns:
        df_clean[col] = pd.to_numeric(df_clean[col].replace('[^0-9.-]', '', regex=True), errors='coerce')

    # Add the customer name column
    df_clean['customer_name'] = customer_name

    # Reset the index and add an 'index' column starting from 1
    df_clean.reset_index(drop=True, inplace=True)
    df_clean['index'] = df_clean.index + 1

    # Reorder columns to have 'index' as the first column
    column_order = ['index'] + columns_2 + ['customer_name']
    df_clean = df_clean[column_order]

    # Save the cleaned DataFrame back to CSV
    df_clean.to_csv(file_path, index=False)
    
    # For debugging: Print the cleaned 'date' column
    print(df_clean['date'])

    return df_clean

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    scores = []
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}

                    # get confidence score
                    scores.append(str(cell['Confidence']))

                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows, scores

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        if "," in word['Text'] and word['Text'].replace(",", "").isnumeric():
                            text += '"' + word['Text'] + '"' + ' '
                        else:
                            text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
    return text

def get_table_csv_results(file_name, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    with open(file_name, 'rb') as file:
        img_test = file.read()
        bytes_test = bytearray(img_test)
        print('Image loaded', file_name)

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name='us-east-1'
    )

    client = session.client('textract')

    response = client.analyze_document(Document={'Bytes': bytes_test}, FeatureTypes=['TABLES'])

    # Get the text blocks
    blocks = response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index + 1)
        csv += '\n\n'

    return csv

def generate_table_csv(table_result, blocks_map, table_index):
    rows, scores = get_rows_columns_map(table_result, blocks_map)

    # Create an in-memory text stream for CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Write the table rows
    for row_index, cols in rows.items():
        row = [cols.get(col_index, '') for col_index in sorted(cols)]
        writer.writerow(row)

    return output.getvalue()

def pdf_to_bronze_csv(pdf_file):
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

    # Convert PDF to PNGs   
    temp_png_dir = './temp_pngs'
    png_files = pdf_to_pngs(pdf_file, temp_png_dir)

    output_file = f'{pdf_file.rsplit(".", 1)[0]}.csv'
    
    # Process each PNG and append to the CSV
    with open(output_file, "wt") as fout:
        for png_file in png_files:
            # Preprocess the image
            # preprocessed_image = preprocess_image(png_file)

            # table_csv = get_table_csv_results(preprocessed_image, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            table_csv = get_table_csv_results(png_file, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
             # Print the headers of the table
            cleaned_csv = clean_table_csv(table_csv)
        
            # Print the cleaned headers
            print_table_headers(cleaned_csv)
            
            fout.write(cleaned_csv)
            fout.write('\n')  # Add a newline between PNG table results
            
            # Remove PNG file after processing
            os.remove(png_file)

    # Cleanup temporary directory
    shutil.rmtree(temp_png_dir)

    cleanup_csv(output_file)  # Cleanup the CSV

    df_filtered = process_file(output_file)

    print(f'CSV OUTPUT FILE: {output_file}')

    # delete the output file
    os.remove(output_file)
    print(f"removed {output_file}")
    os.remove(pdf_file)
    print(f"removed {pdf_file}")
    # os.remove(f'{output_file}.pdf')

    return df_filtered



