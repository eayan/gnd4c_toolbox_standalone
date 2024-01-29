import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import psycopg2
from db.db_functions import db_connect_string

conn_string = db_connect_string()
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

def get_person(date_of_scoring):
	cursor.execute("SELECT * FROM scoring_table WHERE date_of_scoring='{}'" .format(date_of_scoring))
	data = cursor.fetchall()
	return data

def select_unique_person():
	cursor.execute("SELECT DISTINCT date_of_scoring FROM scoring_table")
	data = cursor.fetchall()
	return data	

def edit_single_person_object(new_data_id, new_gnd_id, data_id, gnd_id):
	cursor.execute("UPDATE scoring_table SET data_id=%s, gnd_id=%s WHERE data_id=%s and gnd_id=%s",(new_data_id, new_gnd_id, data_id, gnd_id))
	conn.commit()
	return gnd_id	

def fun_reporting():

    df = pd.read_sql("select * from scoring_table", con=conn)
    df =df.astype(str)
    df['date_of_scoring']=df['date_of_scoring'].replace('None', np.nan)
    df['data_id']=df['data_id'].replace('None', np.nan)
    df['date_of_scoring']=df['date_of_scoring'].fillna(method='ffill')
    df['data_id']=df['data_id'].fillna(method='ffill')
    list_of_persons = [i[0] for i in select_unique_person()]

    selected_person = st.selectbox("Benutzer zu editieren", list_of_persons)
    selected_result = get_person(selected_person)
    
    if selected_result:
        data_id= selected_result[0][1]
        gnd_id= selected_result[0][3]
        col1,col2 = st.columns([2,3])
        with col1:
            st.info("NDS Source Data")
            df=df[df['date_of_scoring']==selected_person]
            df_1=pd.DataFrame(df, columns=['gndIdentifier','id','preferredName','variantName', 'dateOfBirth', 'dateOfDeath','placeOfBirth', 'placeOfDeath', 'professionOrOccupation', 'new_total_score','total_score', 'match_status','comment', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'IstMaxNameScore','IstMx_x_Anz', 'G_birthdate_score', 'H_deathdate_score','O_birthplace_score','P_deathplace_score', 'max_job_score'])
            df =pd.DataFrame(df, columns=['data_id','gnd_id','NDS Id', 'Person Id','Data History','Source System','Entity Type', 'Forename', 'Surname', 'Personal Name', 'Name Addition', 'Counting', 'Prefix','Non-Preferred Name','Preferred', 'Gender', 'Birthdate', 'Deathdate', 'Birthplace', 'Deathplace', 'Profession','Descriptions'])
            new_gnd_id=st.text_input("GND ID Addieren", gnd_id)
            df=df[df['NDS Id']!='None']
            df=df.transpose()
            df=df.iloc[0:]
            st.write(df)
            df=pd.DataFrame(df).transpose()
            gnd_id=df['gnd_id'].values.tolist()
            gnd_id=str(gnd_id[0])
        with col2:
            st.info("GND Target Data")
            data_id=df['data_id'].values.tolist()
            data_id=str(data_id[0])
            new_data_id=data_id
            df_1=df_1.transpose()
            st.write(df_1)
        if st.button("Data Aktualisieren"):
            edit_single_person_object(new_data_id, new_gnd_id, data_id, gnd_id)
            st.success("Erfolgreich Editiert. new_gnd_id: {}".format(new_gnd_id))