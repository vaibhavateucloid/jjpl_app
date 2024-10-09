import csv
import json
import pandas as pd
import streamlit as st
from PyPDF2 import PdfReader
from dotenv import load_dotenv
import time
import streamlit.components.v1 as components
from base64 import b64encode
import io
import xlsxwriter
from customer_pdf_to_bronze import *
from jjpl_ledger_data import *
from databricks import sql
from recon_gold import *

# Set page configuration
st.set_page_config(page_title="LumenAI Reconciler", page_icon="https://s3.amazonaws.com/lumenai.eucloid.com/assets/images/icons/logo.svg", layout="wide", initial_sidebar_state="auto", menu_items=None)

def add_logo_btn1():
    logo_url = "https://lumenai.eucloid.com/assets/images/logo.svg"
    back_button_url = "https://product.lumenai.eucloid.com/home"

    st.sidebar.markdown(
        f"""
        <div style="display: flex; justify-content: flex-start; align-items: center; padding-bottom: 20px;">
            <a href="{back_button_url}" target="_self">
                <img src="https://s3.amazonaws.com/lumenai.eucloid.com/assets/images/icons/back-btn.svg" alt="<-" width="20" height="20" style="margin-right: 10px;">
            </a>
            <div style="text-align: center;">
                <a href="https://product.lumenai.eucloid.com/login" target="_self">
                    <img src="{logo_url}" alt="Logo" width="225" height="fit-content">
                </a>
            </div>
        </div>
    """,
        unsafe_allow_html=True
    )

def remove_existing_files(extension):
    """Remove existing files with the specified extension in the current directory."""
    for filename in os.listdir('.'):
        if filename.endswith(extension):
            os.remove(filename)
            # st.info(f"Removed existing file: {filename}")

def generate_excel_download(df):
    """Generate a downloadable Excel file from a DataFrame."""
    towrite = io.BytesIO()
    df.to_excel(towrite, index=False, engine='xlsxwriter')
    towrite.seek(0)
    b64 = b64encode(towrite.read()).decode()  # Convert to base64
    return f'<a href="data:application/octet-stream;base64,{b64}" download="reconciled_report.xlsx">Download Excel file</a>'

def main():
    # Debug information
    import sys
    import pyspark
    import pandas
    # st.write(f"Python version: {sys.version}")
    # st.write(f"PySpark version: {pyspark.__version__}")
    # st.write(f"Pandas version: {pandas.__version__}")

    add_logo_btn1()

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown("<h3 style='margin-bottom: 10px;'>Instructions</h3>", unsafe_allow_html=True)
    st.sidebar.markdown("""
        <div style="padding-left: 10px; line-height: 1.6;">
            <strong>1.</strong> Upload a file set (PDF and Excel) file using the file uploader below.<br><br>
            <strong>2.</strong> Wait for the model to process the data from pdf file and the excel file.<br><br>
            <strong>3.</strong> Click 'Generate Report' to reconcile data and download the report of non reconciled data.
            <br><br>
        </div>
    """, unsafe_allow_html=True)

    uploaded_files = st.sidebar.file_uploader("Choose up to 2 files", type=['pdf', 'xlsx'], accept_multiple_files=True)

    if uploaded_files:
        if len(uploaded_files) > 2:
            st.warning("Please upload up to 2 files only.")
        else:
            col1, col2 = st.columns(2)
            iframe_style = "width: 100%; height: 600px; overflow: scroll;"
            dataframe_height = 600

            for i, uploaded_file in enumerate(uploaded_files):
                file_bytes = uploaded_file.read()

                if uploaded_file.name.endswith('.pdf'):
                    pdf_path = f"./temp_{uploaded_file.name}"
                    with open(pdf_path, "wb") as f:
                        f.write(file_bytes)

                    base64_pdf = b64encode(file_bytes).decode('utf-8')
                    iframe_style = "width: 100%; height: 600px; border: none;"
                    # pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" style="{iframe_style}" type="application/pdf">PDF Viewer</iframe>'
                    pdf_display = f'''
                                <iframe src="data:application/pdf;base64,{base64_pdf}" style="{iframe_style}" type="application/pdf">
                                    PDF Viewer
                                </iframe>
                                '''
                    if i == 0:
                        col1.markdown(f"##### {uploaded_file.name} (File {i + 1}):")
                        col1.markdown(pdf_display, unsafe_allow_html=True)
                    else:
                        col2.markdown(f"##### {uploaded_file.name} (File {i + 1}):")
                        col2.markdown(pdf_display, unsafe_allow_html=True)

                elif uploaded_file.name.endswith('.xlsx'):
                    df = pd.read_excel(uploaded_file)
                    vdf = pd.read_excel(uploaded_file, header=1)

                    sap_excel_file_path = f"./SAP_{uploaded_file.name}"
                    with open(sap_excel_file_path, "wb") as f:
                        f.write(file_bytes)

                    if i == 0:
                        col1.markdown(f"##### {uploaded_file.name} (File {i + 1}):")
                        col1.dataframe(vdf, height=dataframe_height)
                    else:
                        col2.markdown(f"##### {uploaded_file.name} (File {i + 1}):")
                        col2.dataframe(vdf, height=dataframe_height)

            if st.button("Generate Report"):
                if len(uploaded_files) == 2:
                    try:
                        try:
                            silver_df_customer = pdf_to_bronze_csv(pdf_path)
                        except UnboundLocalError:
                            st.error("Please upload the PDF file.")
                            remove_existing_files('.pdf')
                            remove_existing_files('.xlsx')
                            return 
                        
                        try:
                            sdf_cleaned = clean_ledger_data(sap_excel_file_path)
                        except:
                            st.error("Please upload the Excel file.")
                            remove_existing_files('.pdf')
                            remove_existing_files('.xlsx')
                            return 
                        
                        spark_silver_customer_df = pandas_to_spark_silver_customer(silver_df_customer)
                        # st.write("SIlver Customer DF")
                        spark_cleaned_df = pandas_to_spark_cleaned(sdf_cleaned)

                                    # Perform reconciliation and capture messages
                        df_gold_pd, messages = perform_reconciliation(spark_silver_customer_df, spark_cleaned_df)

                        # If df_gold_pd is not empty
                        if df_gold_pd is not None and not df_gold_pd.empty:
                            st.success("Reconciliation completed!")
                            st.write(df_gold_pd)

                            # Display reconciliation messages
                            for message in messages:
                                st.info(message)  # Display messages as info
                            download_link = generate_excel_download(df_gold_pd)
                            st.markdown(download_link, unsafe_allow_html=True)

                        elif df_gold_pd is not None and df_gold_pd.empty == True:
                            for message in messages:
                                st.info(message) 
                            remove_existing_files('.pdf')
                            remove_existing_files('.xlsx')
                    except Exception as e:
                        st.error(f"An error occurred during reconciliation: {str(e)}")
                        st.exception(e)
                        remove_existing_files('.pdf')
                        remove_existing_files('.xlsx')
                else:
                    st.error("Please upload 2 files for reconciliation.")

if __name__ == "__main__":
    main()