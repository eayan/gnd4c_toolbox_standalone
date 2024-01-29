
import pandas as pd
import streamlit as st
import pygwalker as pyg
from io import BytesIO

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config_str = config_file.read()
        return config_str

def fun_data_profiling_person(data):
    data['alternative_title']=data['alternative_title'].astype(str)
    df=data.loc[data['alternative_title']!='None']
    grouped = df.groupby(['alternative_title'])
    group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
    objects = st.container()
    object_list = df['alternative_title'].unique()
    choice = objects.selectbox("Objekt Wählen", object_list)
    df=df.loc[(df['alternative_title'] == choice)]
    columns=['dc_id','forename','surname', 'personal_name','name_addition','prefix', 'counting','gender', 'birth_date', 'death_date', 'non_preferred_name_surname', 'birth_place death_place period_of_activity_start','period_of_activity_end', 'profession']
    columns = [col for col in columns if col in df.columns]
    df = df[columns]
    
    if choice:
        st.write("ausgewählte private Daten:", choice)
        df_pywalk=df

        with st.expander('General Overview'):
            df=df.describe(include=[object]).transpose()
            st.table(df)
        with st.expander('Data'):
            config= load_config('config/config_data_profiling_person.json') # Replace with your own folder path
            pyg.walk(df_pywalk, env='Streamlit', dark='light', spec=config)

def fun_data_profiling_building(data):
    data['alternative_title']=data['alternative_title'].astype(str)
    df=data.loc[data['alternative_title']!='None']
    grouped = df.groupby(['alternative_title'])
    group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
    objects = st.container()
    object_list = df['alternative_title'].unique()
    choice = objects.selectbox("Objekt Wählen", object_list)
    df=df.loc[(df['alternative_title'] == choice)]
    columns=columns=['dc_identifier','gnd_identifier','alternate_name','abbreviated_name',' place_name','production_date preferred_name','variant_name',
'related_place_name','professional_relationship','biographical_information','subject_category','related_subject_heading' 'geographic_area_code',
'place_literal','place_gnd_identifier','hierarchical_place_name_literal', 'address_type','street_address','postal_code geo_type','latitude',
'longitude','broader_term_instantial_literal', 'broader_term_instantial_gnd_identifier','data_id default_title','date_of_import','alternative_title',
'comment_content', 'imported_by']
    columns = [col for col in columns if col in df.columns]
    df = df[columns]
    
    if choice:
        st.write("ausgewählte private Daten:", choice)
        df_pywalk=df

        with st.expander('General Overview'):
            df=df.describe(include=[object]).transpose()
            st.table(df)
        with st.expander('Data'):
            config= load_config('config/config_data_profiling_building.json') # Replace with your own folder paths
            pyg.walk(df_pywalk, env='Streamlit', dark='light', spec=config)

def fun_data_profiling_building(data):
    data['alternative_title']=data['alternative_title'].astype(str)
    df=data.loc[data['alternative_title']!='None']
    grouped = df.groupby(['alternative_title'])
    group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
    objects = st.container()
    object_list = df['alternative_title'].unique()
    choice = objects.selectbox("Objekt Wählen", object_list)
    df=df.loc[(df['alternative_title'] == choice)]
    columns=columns=['dc_identifier','gnd_identifier','alternate_name','abbreviated_name',' place_name','production_date preferred_name','variant_name',
'related_place_name','professional_relationship','biographical_information','subject_category','related_subject_heading' 'geographic_area_code',
'place_literal','place_gnd_identifier','hierarchical_place_name_literal', 'address_type','street_address','postal_code geo_type','latitude',
'longitude','broader_term_instantial_literal', 'broader_term_instantial_gnd_identifier','data_id default_title','date_of_import','alternative_title',
'comment_content', 'imported_by']
    columns = [col for col in columns if col in df.columns]
    df = df[columns]
    if choice:
        st.write("ausgewählte private Daten:", choice)
        df_pywalk=df
        with st.expander('General Overview'):
            df=df.describe(include=[object]).transpose()
            st.table(df)
        with st.expander('Data'):
            config= load_config('config/config_data_profiling_building.json') # Replace with your own folder path
            pyg.walk(df_pywalk, env='Streamlit', dark='light', spec=config)
