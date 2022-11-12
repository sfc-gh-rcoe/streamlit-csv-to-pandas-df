import pandas as pd
import streamlit as st

def introduce_app():
	st.title("Welcome to the file uploader")

def get_a_file():
	theFile = st.file_uploader("Locate the file to be uploaded")
	if theFile is not None:
		return theFile
	

introduce_app()

df = pd.read_csv(get_a_file())
if df is not None:
	st.table(df)
