import pandas as pd
import streamlit as st

def introduce_app():
	st.title("Welcome to the file uploader")

def get_a_file():
	theFile = st.file_uploader("Locate the file to be uploaded")
	if theFile is not None:
		return theFile
	

introduce_app()

r_theFile = get_a_file()
if r_theFile is not None:
	df = pd.read_csv(r_theFile, header=None, names=['transactionDate', 'transactionAmount', 'transactionStatus'])
	st.table(df)
