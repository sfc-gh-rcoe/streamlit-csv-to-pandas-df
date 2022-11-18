import pandas as pd
import streamlit as st
import snowflake.connector
from datetime import datetime
from snowflake.snowpark.session import Session



def create_sp_session():
  conn_param = {
    "account": st.secrets["snowflake"].account,
    "user": st.secrets["snowflake"].user,
    "database": st.secrets["snowflake"].database,
    "role": st.secrets["snowflake"].role,
    "warehouse": st.secrets["snowflake"].warehouse,
    "schema": st.secrets["snowflake"].schema,
    "password": st.secrets["snowflake"].password
  }
  session = Session.builder.configs(conn_param).create()
  return session

def create_snow_table(s_sess):
#	snp_session.use_database(st.secrets["snowflake"].database)
#	snp_session.use_role(st.secrets["snowflake"].role)
#	snp_session.use_schema(st.secrets["snowflake"].schema)
#	snp_session.use_warehouse(st.secrets["snowflake"].warehouse)
	now = datetime.now()
	t_stamp = now.strftime("%H%M%S")
	df_snp = s_sess.createDataFrame(df)
	df_snp.write.mode('Overwrite').save_as_table("table_one_gb_" + t_stamp)

def grant_header_names(t_df):
	n_cols = df.shape[1]
	for i in range(n_cols):
		st.text_input("Name for column " + str(i))
	st.button("Apply column nams", on_click=apply_header_names)

def apply_header_names(a_column_names):
	# Implement logic here
	pass

def introduce_app():
	st.title("Welcome to the file uploader")

def get_a_file():
	theFile = st.file_uploader("Locate the file to be uploaded")
	if theFile is not None:
		return theFile
	

introduce_app()

r_theFile = get_a_file()
if r_theFile is not None:
	df = pd.read_csv(r_theFile, header=None)
	st.table(df)
	snp_session = create_sp_session()
	n_cols = df.shape[1]
	st.write("This table has " + str(n_cols) + " columns.")
	b_hasheader = st.checkbox("Table has a header row?")
	if (not b_hasheader):
		#need to fill in form for column names
		grant_header_names(df)
	else:
		create_snow_table(snp_session)

