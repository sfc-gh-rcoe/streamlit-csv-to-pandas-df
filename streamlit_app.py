import streamlit as st
import pandas as pd
import snowflake.connector
import numpy as np
from datetime import datetime
from snowflake.snowpark.session import Session
from io import StringIO



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

def create_snow_table(s_sess, t_df):
#	snp_session.use_database(st.secrets["snowflake"].database)
#	snp_session.use_role(st.secrets["snowflake"].role)
#	snp_session.use_schema(st.secrets["snowflake"].schema)
#	snp_session.use_warehouse(st.secrets["snowflake"].warehouse)
	now = datetime.now()
	t_stamp = now.strftime("%H%M%S")
	t_newNames = {}
	n_cols = t_df.shape[1]
	for j in range(n_cols):
		st.write(t_df[j][0])
		t_newNames.update("{j: t_df[0][j]}")
	st.write(t_newNames)
	t_df.rename(columns=t_newNames, inplace=True)
	st.table(t_df)
#	df_snp = s_sess.createDataFrame(t_df)
#	df_snp.write.mode('Overwrite').save_as_table("table_one_gb_" + t_stamp)

def inspect_for_header(t_df, t_newNames):
	#Inspect data frame for possible column headers
	n_col = int(t_df.shape[1])
	st.table(t_df)
	st.write(str(n_col))
	for j in range(n_col):
		# t_newNames.update({j: t_df[j]})
		t_newNames.update({j: t_df[j][0]})
	return t_newNames

def grant_header_names(t_df):
	n_cols = len(st.session_state)
	t_colNames = {}
	for i in range(n_cols):
		t_colNames.update({i: st.session_state["field_" + str(i)]})
	t_df.rename(columns=t_colNames, inplace=True)
	return t_df

def apply_header_names(a_df):
	# Implement logic here
	n_cols = int(a_df.shape[1])
	t_colNames = {}
	for i in range(n_cols):
		t_colNames.update({i: a_df[i][0]})
	a_df.rename(columns=t_colNames, inplace=True)
	return a_df
		

def introduce_app():
	st.title("Welcome to the file uploader")

def get_a_file():
	theFile = st.file_uploader("Locate the file to be uploaded")
	if theFile is not None:
		return theFile

def stage_field_names(t_index, t_fieldName):
#	st.session_state[t_fieldName] = t_fieldName
	st.write(st.session_state)
	

introduce_app()

r_theFile = get_a_file()
b_hasheader = False
t_newNames = {}
encoding = 'utf-8'
try:
	t_dataBuffer = r_theFile.read()
except:
	st.write("no current file selected")
n_df = pd.DataFrame()
if r_theFile is not None:
	df = pd.read_csv(StringIO(str(t_dataBuffer, encoding)), header=None)
	c_headers = inspect_for_header(df, t_newNames)
	st.write(c_headers)
	st.write("Does the above output look to be column headers?")
	r_options = ('yes', 'no')
	b_headers = st.radio("Column headers?", (r_options), 1)
	st.write(b_headers)
	if (b_headers != 'yes'):
#		df = pd.read_csv(StringIO(str(t_dataBuffer, encoding)), header=None)
		df = pd.DataFrame(df)
		for k in range(len(c_headers)):
			# st.text_input("Name for column " + str(k) + ":", on_change=stage_field_names, key="field_" + str(k), args=(k, 'field_' + str(k)))
			st.text_input("Name for column " + str(k) + ":", key="field_" + str(k), value="field_" + str(k) )
		n_df = grant_header_names(df)
	else:
#		df = pd.read_csv(StringIO(str(t_dataBuffer, encoding)), header=1, skiprows=1)
		df = apply_header_names(df)
		n_df = df.drop([0, 0])
	st.table(n_df)
	#snp_session = create_sp_session()
	#n_cols = n_df.shape[1]
	#st.write("This table has " + str(n_cols) + " columns.")
	#b_hasheader = st.checkbox("Table has a header row?")
	#if (not b_hasheader):
	#	#need to fill in form for column names
	#	grant_header_names(df)
	#else:
	st.radio("Create Snowflake Table?", (r_options), 1)
	if b_createSnowTable:
		create_snow_table(snp_session, df)

