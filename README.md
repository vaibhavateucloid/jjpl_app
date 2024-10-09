# LumenAI Reconciler

LumenAI Reconciler is a Streamlit-based web application designed to reconcile ledger data from different sources. It processes PDF and Excel files, performs data cleaning and reconciliation, and generates reports.

## Features

- PDF and Excel file processing
- Data cleaning and normalization
- Ledger reconciliation
- Report generation
- Dockerized application for easy deployment

## Prerequisites

- Docker

## Installation

1. Clone this repository:
   ```
   git clone git@github.com:vaibhavateucloid/jjpl_app.git
   cd lumenai-reconciler
   ```

2. Build the Docker image:
   ```
   docker build -t lumenai-reconciler .
   ```

## Usage

1. Run the Docker container:
   ```
   docker run -p 8501:8501 lumenai-reconciler
   ```

2. Open your web browser and navigate to `http://localhost:8501`

3. Use the sidebar to upload your PDF and Excel files.

4. Follow the on-screen instructions to process the files and generate reconciliation reports.

## File Structure

- `app.py`: Main Streamlit application file
- `customer_pdf_to_bronze.py`: PDF processing logic
- `customer_bronze_to_silver.py`: Data cleaning for customer data
- `jjpl_ledger_data.py`: Processing for JJPL ledger data
- `recon_gold.py`: Reconciliation logic
- `requirements.txt`: Python dependencies
- `Dockerfile`: Instructions for building the Docker image

## Environment Variables

The application uses environment variables for AWS credentials. Make sure to set the following variables in a `.env` file or in your environment:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `JAVA_HOME`
