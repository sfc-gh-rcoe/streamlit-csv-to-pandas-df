import pandas as pd
import streamlit as st

def introduce_app():
	st.title("Welcome to the file uploader")

def get_a_file():
	return st.file_uploader("Locate the file to be uploaded")

introduce_app()

df = pd.read_csv(get_a_file())
st.table(df)
