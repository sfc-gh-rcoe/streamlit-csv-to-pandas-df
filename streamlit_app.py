import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.snowpark.session import Session



def create_sp_session():
  conn_param = {
    "account": st.secrets["snowflake"].account,
    "user": st.secrets["snowflake"].user,
    "databasae": st.secrets["snowflake"].database,
    "role": st.secrets["snowflake"].role,
    "warehouse": st.secrets["snowflake"].warehouse,
    "schema": st.secrets["snowflake"].schema,
    "password": st.secrets["snowflake"].password
  }
  session = Session.builder.configs(conn_param).create()
  return session

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
	snp_session = create_sp_session()
	df_snp = snp_session.CreateDataFrame(df)
	df_snp.write.mode('Overwrite').save_as_table("table_one_gb")




