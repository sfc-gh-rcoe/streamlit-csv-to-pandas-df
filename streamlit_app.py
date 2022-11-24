import streamlit as st
import pandas as pd
import snowflake.connector
import numpy as np
import re
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

def create_snow_table(s_sess, t_df, theTableName):
	now = datetime.now()
	t_stamp = now.strftime("%H%M%S")
	df_snp = s_sess.createDataFrame(t_df)
	df_snp.write.mode('Overwrite').save_as_table(theTableName + "_" + t_stamp)

def inspect_for_header(t_df, t_newNames):
	#Inspect data frame for possible column headers
	n_col = int(t_df.shape[1])
	st.table(t_df)
	# st.write(str(n_col))
	for j in range(n_col):
		# t_newNames.update({j: t_df[j]})
		t_txt = t_df[j][0]
		if t_txt.isnumeric():
			t_newNames.update({j: t_df[j][0]})
		else:
			t_newNames.update({j: "'" + t_df[j][0] + "'"})
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
	st.title("Snowflake data ingest")

def get_a_file():
	theFile = st.sidebar.file_uploader("Locate the file to be uploaded")
	if theFile is not None:
		return theFile

def stage_field_names(t_index, t_fieldName):
#	st.session_state[t_fieldName] = t_fieldName
	st.write(st.session_state)
	
def inspect_file_name(p_fileName):
	t_validFileExt = st.secrets["supportedfiles"].filetypes
	t_fileName = p_fileName.split(".")
	# st.write(str(len(t_fileName)))
	if t_fileName[len(t_fileName)-1] in t_validFileExt:
		return True
	else:
		return False
	
def get_file_type(p_fileName):
	t_fileName = p_fileName.split(".")
	return t_fileName[len(t_fileName)-1]

introduce_app()


c1, c2, c3 = st.columns(3, gap = 'large')
r_options = ('yes', 'no')
r_theFile = get_a_file()
c_headers = None

with c1:
	b_hasheader = False
	b_createSnowTable = False
	t_newNames = {}
	encoding = 'utf-8'
	try:
		t_dataBuffer = r_theFile.read()
		r_theFileName = r_theFile.name
	except:
		st.write("no current file selected")
	n_df = pd.DataFrame()
	if r_theFile is not None:
		if inspect_file_name(r_theFileName):
			if get_file_type(r_theFileName) == 'csv':
				df = pd.read_csv(StringIO(str(t_dataBuffer, encoding)), header=None)
			elif get_file_type(r_theFileName) == 'json':
				df = pd.read_json(StringIO(str(t_dataBuffer, encoding)), orient='index')
				
			c_headers = inspect_for_header(df, t_newNames)
			with c3:
				st.write(c_headers)
				st.write("Does the above output look to be column headers?")
				b_headers = st.radio("Column headers?", (r_options), 1)
				st.write(b_headers)
			if (b_headers != 'yes'):
				df = pd.DataFrame(df)
				for k in range(len(c_headers)):
					# st.text_input("Name for column " + str(k) + ":", on_change=stage_field_names, key="field_" + str(k), args=(k, 'field_' + str(k)))
					with c3:
						st.text_input("Name for column " + str(k) + ":", key="field_" + str(k), value="field_" + str(k) )
				n_df = grant_header_names(df)
			else:
				df = apply_header_names(df)
				n_df = df.drop([0, 0])
		else:
			st.write("You haven't selected a usable file")
with c2:
	st.table(n_df)
with c3:
	b_createSnowTable = st.radio("Create Snowflake Table?", (r_options), 1)
	if (b_createSnowTable == 'yes'):
		snp_session = create_sp_session()
		r_theFileName = re.sub('[.-]', '_', r_theFileName)
		create_snow_table(snp_session, n_df, r_theFileName)

