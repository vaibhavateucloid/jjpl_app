import csv
import json
# import docx
# import mammoth
# from io import StringIO
# import io
import pandas as pd
# import streamlit as st
# import requests
# from pdf2image import convert_from_bytes
import streamlit as st
from PyPDF2 import PdfReader  # library to read pdf files
# from langchain.text_splitter import RecursiveCharacterTextSplitter  # library to split pdf files
# import os
# from langchain_google_genai import GoogleGenerativeAIEmbeddings  # to embed the text
# import google.generativeai as genai
# from langchain.vectorstores import FAISS  # for vector embeddings
# from langchain_google_genai import ChatGoogleGenerativeAI  #
# from langchain.chains.question_answering import load_qa_chain  # to chain the prompts
# from langchain.prompts import PromptTemplate  # to create prompt templates
from dotenv import load_dotenv
import time
from streamlit_extras.app_logo import add_logo
import streamlit.components.v1 as components
import base64
import io

st.set_page_config(page_title="LumenAI Extractor", page_icon="📈" ,layout="wide", initial_sidebar_state="auto", menu_items=None)
# Set page configuration
# st.set_page_config(page_title="Your App Title", layout="wide")


def flatten(data):
    flat_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flat_data.update(flatten(value))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                flat_data.update(flatten({f"{key}_{i + 1}_{k}": v for k, v in item.items()}))
        else:
            flat_data[key] = value
    return flat_data



def get_pdf_text(pdf_docs):
    text = ""
    # iterate over all pdf files uploaded
    # for pdf in pdf_docs:
    pdf_reader = PdfReader(pdf_docs)
    # iterate over all pages in a pdf
    for page in pdf_reader.pages:
        text += page.extract_text()
    return text


# def get_text_chunks(text):
#     # create an object of RecursiveCharacterTextSplitter with specific chunk size and overlap size
#     text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=1000)
#     # now split the text we have using object created
#     chunks = text_splitter.split_text(text)

#     return chunks


# def get_vector_store(text_chunks):
#     embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key=api_key)  # google embeddings
#     vector_store = FAISS.from_texts(text_chunks,
#                                     embeddings)  # use the embedding object on the splitted text of pdf docs
#     vector_store.save_local("faiss_index")  # save the embeddings in local

def add_logo_btn1():
    logo_url = "https://lumenai.eucloid.com/assets/images/logo.svg"
    back_button_url = "https://product.lumenai.eucloid.com/home"

    # Wrapping the back button and logo inside a flex container to align them better
    st.sidebar.markdown(
        f"""
        <div style="display: flex; justify-content: space-between; align-items: center; padding-bottom: 20px;">
            <!-- Back button -->
            <a href="{back_button_url}" target="_self">
                <img src="https://s3.amazonaws.com/lumenai.eucloid.com/assets/images/icons/back-btn.svg" alt="<-" width="20" height="20" style="margin-right: 10px;">
            </a>
            <!-- Centered Logo -->
            <div style="text-align: center;">
                <img src="{logo_url}" alt="Logo" width="120" height="120">
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def reconcile_data(file1, file2):
    # Dummy reconciliation logic, replace with actual reconciliation process
    # For now, assume both files are Excel and we are merging them based on some common key (like "ID")
    
    df1 = pd.read_excel(file1) if file1.name.endswith('.xlsx') else None
    df2 = pd.read_excel(file2) if file2.name.endswith('.xlsx') else None
    
    # Example reconciliation: merging two dataframes on a common column (e.g., "ID")
    if df1 is not None and df2 is not None:
        merged_df = pd.merge(df1, df2, on='ID', how='inner')  # Modify this as per your reconciliation logic
        return merged_df
    else:
        st.error("Reconciliation logic for PDFs needs to be added.")
        return None

def generate_excel_download(df):
    # Function to generate a downloadable Excel file from the dataframe
    towrite = io.BytesIO()
    df.to_excel(towrite, encoding='utf-8', index=False, engine='xlsxwriter')
    towrite.seek(0)
    b64 = base64.b64encode(towrite.read()).decode()  # some strings <-> bytes conversions necessary here
    return f'<a href="data:application/octet-stream;base64,{b64}" download="reconciled_report.xlsx">Download Excel file</a>'

def main():
    st.sidebar.header("LUMEN AI")
    add_logo_btn1()

    # Adding spacing and padding to the instruction section
    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown("<h3 style='margin-bottom: 10px;'>Instructions</h3>", unsafe_allow_html=True)
    st.sidebar.markdown("""
        <div style="padding-left: 10px; line-height: 1.6;">
            <strong>1.</strong> Upload a PDF or Excel file using the file uploader below.<br><br>
            <strong>2.</strong> Wait for the model to process the image.<br><br>
            <strong>3.</strong> Click 'Generate Report' to reconcile data and download the report.
        </div>
    """, unsafe_allow_html=True)

    uploaded_files = st.sidebar.file_uploader("Choose up to 2 files", type=['pdf', 'xlsx'], accept_multiple_files=True)

    if uploaded_files:
        if len(uploaded_files) > 2:
            st.warning("Please upload up to 2 files only.")
        else:
            # Create two columns with equal width to consume all the space except for the sidebar
            col1, col2 = st.columns(2)  # Equal width for both columns
            iframe_style = "width: 100%; height: 600px; overflow: scroll;"  # Same height, full width for PDFs
            dataframe_height = 600  # Same height for dataframes

            # Loop through the uploaded files and display them in the respective columns
            for i, uploaded_file in enumerate(uploaded_files):
                file_bytes = uploaded_file.read()

                # Display PDF file
                if uploaded_file.name.endswith('.pdf'):
                    base64_pdf = base64.b64encode(file_bytes).decode('utf-8')
                    pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" style="{iframe_style}" type="application/pdf">PDF Viewer</iframe>'

                    # Choose the correct column to display the file
                    if i == 0:
                        col1.markdown(f"##### Uploaded Document {i+1}:")
                        col1.markdown(pdf_display, unsafe_allow_html=True)
                    else:
                        col2.markdown(f"##### Uploaded Document {i+1}:")
                        col2.markdown(pdf_display, unsafe_allow_html=True)

                # Display Excel (.xlsx) file
                elif uploaded_file.name.endswith('.xlsx'):
                    df = pd.read_excel(uploaded_file)

                    # Choose the correct column to display the file
                    if i == 0:
                        col1.markdown(f"##### Uploaded Excel File {i+1}:")
                        col1.dataframe(df, height=dataframe_height)
                    else:
                        col2.markdown(f"##### Uploaded Excel File {i+1}:")
                        col2.dataframe(df, height=dataframe_height)

            # Add the "Generate Report" button and reconciliation logic
            if st.button("Generate Report"):
                if len(uploaded_files) == 2:
                    reconciled_data = reconcile_data(uploaded_files[0], uploaded_files[1])
                    if reconciled_data is not None:
                        st.success("Reconciliation completed!")
                        st.write(reconciled_data)  # Show reconciled data
                        
                        # Generate the downloadable Excel file
                        download_link = generate_excel_download(reconciled_data)
                        st.markdown(download_link, unsafe_allow_html=True)
                else:
                    st.error("Please upload 2 files for reconciliation.")

if __name__ == "__main__":
    main()
