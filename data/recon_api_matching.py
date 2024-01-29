import requests
import streamlit as st
import urllib.request
import json
import pandas as pd
from cgi import test
from cmath import nan
import jellyfish
from similar_text import similar_text
from curses import keyname
from pyparsing import And
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.parse import quote
import urllib.parse
from glom import glom
import json
from IPython.core.display import display, HTML
from requests import request
import grequests
from st_btn_select import st_btn_select
import re
import string
from string import digits
import numpy as np
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import datetime
from sqlalchemy import create_engine
import dask.dataframe as dd
import os
import plotly.express as px
import itertools
from Levenshtein import distance
import ijson
from st_aggrid import AgGrid, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid import JsCode
import psycopg2
import uuid
from itertools import combinations, count
from users.session_state import session_state
from db.db_functions import db_connect


def remove_extra_spaces(input_string):
    return ' '.join(filter(None, input_string.split(' ')))

@st.cache_data
def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text

@st.cache_data
def Convert(string):
    li = list(string.split(" "))
    return li

@st.cache_data
def clean_roman(text):
    pattern = r"\b(?=[MDCLXVIΙ])M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})([IΙ]X|[IΙ]V|V?[IΙ]{0,3})\b\.?"
    return re.sub(pattern, '&', text)


@st.cache_data
def Convert(string):
    li = list(string.split(" "))
    return li
@st.cache_data
def Convert_str_list(string):
    li = list(string.split(","))
    return li

def meta__data(data_source, username):
    now = datetime.datetime.now()
    type_entity ="Person"
    date_of_query = now.strftime("%d/%m/%Y %H:%M:%S")
    st.info("Matching-Ereignis definieren")
    st.write("Erstellt am:", date_of_query)
    st.write("Zielentität:", type_entity)
    st.write("Datenlieferung:", data_source)
    default_title= data_source + type_entity + date_of_query
    st.write("Default Title:", default_title)
    st.write("Query von:", username)
    alternative_title = st.text_input("Geben Sie ein alternative Titel ein")
    comment_content = st.text_area("Geben Sie hier Ihre Kommentar ein", height=50)
    date_of_query = [date_of_query]
    default_title = [default_title]
    alternative_title =[alternative_title]
    comment_content =[comment_content]
    queried_by=[username]
    date_of_query = pd.DataFrame(date_of_query, columns=['date_of_query'])
    default_title = pd.DataFrame(default_title, columns=['default_title'])
    alternative_title =pd.DataFrame(alternative_title, columns=['alternative_title'])
    comment_content =pd.DataFrame(comment_content, columns=['comment_content'])
    queried_by=pd.DataFrame(queried_by, columns=["queried_by"])
    meta__data=pd.concat([date_of_query,default_title,queried_by,alternative_title, comment_content], axis=1)
    return meta__data

def convert_df(all_scores_table_total):
    all_scores_table_total=all_scores_table_total.reset_index()
    return all_scores_table_total.to_csv(index=True).encode('utf-8')

def fn_private_data_reconapi(data):
    username=session_state.username
    data['alternative_title']=data['alternative_title'].astype(str)
    df=data.loc[data['alternative_title']!='None']
    grouped = df.groupby(['alternative_title'])
    group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
    objects = st.container()
    object_list = df['alternative_title'].unique()
    choice = objects.selectbox("Objekt Wählen", object_list)
    if choice:
        st.write("ausgewählte private Daten:", choice)
        ids=uuid.uuid4()
        rows_count = df.count()[0]
        rows_count == df[df.columns[0]].count()
        st.warning("Bitte wählen Sie, ob Sie die Ergebnisse speichern möchten.")
        save_status = st.radio("Ergebnisse speichern.", ('Speichern', 'Nicht speichern'), key="save_status_person_data")        
        data_source=str(choice)
        if save_status == 'Speichern':
            meta_data=meta__data(data_source,username)
            agree = st.checkbox("Matching-Ereignis ist nur für Sie sichtbar")
            if agree:
                st.write("Matching-Ereignis ist nur für Sie sichtbar")
                meta_data['public']="False"
            else:
                meta_data['public']="True"
        elif save_status == 'Nicht speichern':
            all_scores_table_total = []
            meta_data=meta__data(data_source,username)
        else:
            pass

        key_id=uuid.uuid4()
        key_id=str(key_id)

        limit_type = st.radio("Limit", ('Standard', 'Random'))
        df = grouped.get_group(choice)
        df['index'] =df['index'].astype(int)
        df =df.sort_values(by='index', ascending=True)
        rows_count = df.count()[0]
        rows_count == df[df.columns[0]].count()
        st.write("Die Gesamtanzahl der Zeilen in diesem Datensatz beträgt:", rows_count)

        limit_slider_standard=""
        limit_slider_random =""

        if limit_type == 'Standard':
            limit = limit_type
            range_start = st.number_input('Range Start', value=0)
            range_end = st.number_input('Range End', value=(int(rows_count)))
            slider_range=st.slider("double ended slider", value=[range_start, range_end])
            st.write("slider range:", slider_range, slider_range[0], slider_range[1])
            slider_range_1=slider_range[0]
            slider_range_2=slider_range[1]
            candidates=df.iloc[slider_range_1:slider_range_2]
            selected_rows_count = candidates[candidates.columns[0]].count()
            limit_slider_standard =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))

        elif limit_type == 'Random':
            limit = limit_type
            range_start = st.number_input('Range Start', value=0)
            range_end = st.number_input('Range End', value=(int(rows_count)))
            slider_range=st.slider("double ended slider", value=[range_start, range_end])
            st.write("slider range:", slider_range, slider_range[0], slider_range[1])
            slider_range_1=slider_range[0]
            slider_range_2=slider_range[1]
            candidates=df.iloc[slider_range_1:slider_range_2]
            selected_rows_count = candidates[candidates.columns[0]].count()
            limit_slider_random =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))

        if limit_slider_standard:
            query_candidates=candidates.iloc[0:limit_slider_standard]
            query_candidates = st.data_editor(query_candidates)
            query_candidates =query_candidates.astype(str)

        elif limit_slider_random:
            query_candidates = candidates.sample(n=limit_slider_random, replace=True, random_state=1)
            query_candidates = st.data_editor(query_candidates)
            query_candidates =query_candidates.astype(str)
            query_candidates =query_candidates.drop_duplicates(subset=['dc_id'],keep='first')

        st.info("Bitte Wählen Lobid Filter Type")
        selected = st.radio("Bitte Wählen", ('Preferred Names', 'nach Personennamen filtern', 'nach Objekt Id filtern'))
        if selected == 'Preferred Names':
            query_candidates =query_candidates.astype(str)
        
        elif selected == 'nach Personennamen filtern':
            query_candidates =query_candidates.astype(str)
            options =query_candidates['surname'].unique()
            selected_options =st.multiselect("Name Wählen", options)
            query_candidates = query_candidates.loc[query_candidates["surname"].isin(selected_options)]

        elif selected == 'nach Objekt Id filtern':                    
            query_candidates =query_candidates.astype(str)
            options =query_candidates['dc_id'].unique()
            selected_options =st.multiselect("Objekt Id Wählen", options)
            query_candidates = query_candidates.loc[query_candidates["dc_id"].isin(selected_options)]
        else:
            pass
        
        st.info('Bitte Wählen Lobid Query Size Limit')
        size_limit_0= '10'
        size_limit_1= '20'
        size_limit_2= '40'
        size_limit_3= '60'
        size_limit_4= '80'
        size_limit_5= '100'
        lobid_size_limit = st.radio("Size Limit", (size_limit_0, size_limit_1, size_limit_2, size_limit_3, size_limit_4, size_limit_5), horizontal=True, label_visibility="visible", key="size_type")
        
        limit_size =""
        if lobid_size_limit == size_limit_0:
            limit_size=size_limit_0
        elif lobid_size_limit == size_limit_1:
            limit_size=size_limit_1
        elif lobid_size_limit == size_limit_2:
            limit_size=size_limit_2
        elif lobid_size_limit == size_limit_3:
            limit_size=size_limit_3
        elif lobid_size_limit == size_limit_4:
            limit_size=size_limit_4
        elif lobid_size_limit == size_limit_5:
            limit_size=size_limit_5 
                                
        if st.button("Load Data"):
            st.session_state.load_state = True
            start_time = time.time()
            def tic():
                global start_time 
                start_time = time.time()

            lev_distance = "~ "
            for row in query_candidates.values:
                index_id = str(row[1])
                dc_id= str(row[2])
                NDS_Id= str(row[2])
                person_id= str(row[2])
                forename=str(row[3]).strip()
                forename_org=forename
                surname=str(row[4]).strip()
                surname_org=surname
                personal_name=str(row[5]).strip()
                personal_name_org=personal_name
                name_addition=str(row[6]).strip()
                name_addition_org=name_addition
                prefix=str(row[7]).strip()
                prefix_org=prefix
                counting=str(row[8]).strip()
                counting_org=counting
                gender=str(row[9]).strip()
                gender_org=gender
                birthdate =str(row[10]).strip()
                birthdate_org=birthdate
                deathdate =str(row[11]).strip()
                deathdate_org=deathdate
                non_preferred_names= str(row[13]).strip()
                non_preferred_names_org=non_preferred_names
                so_placeOfBirth=str(row[14]).strip()
                so_placeOfBirth_org=so_placeOfBirth
                so_placeOfDeath=str(row[15]).strip()
                so_placeOfDeath_org=so_placeOfDeath
                so_period_of_activities_start=str(row[16]).strip()
                so_period_of_activities_start_org=so_period_of_activities_start
                so_period_of_activities_end=str(row[17]).strip()
                so_period_of_activities_end_org=so_period_of_activities_end
                so_profession=str(row[18]).strip()
                so_profession_org=so_profession

                data_id=str(row[19]).strip()
                dataset_id=str(row[19]).strip()
                
                forename_surname_org= " "

                if forename != "None" and surname != "None":
                    forename_surname_org=forename+' '+surname
                if forename == "None" and surname != "None":
                    forename_surname_org=surname
                if forename != "None" and surname == "None":
                    forename_surname_org=forename
                if forename == "None" and surname == "None":
                    forename_surname_org=personal_name
                if forename == "None" and surname  == "None" and personal_name == "None" :
                    forename_surname_org=name_addition

                if non_preferred_names != "None None":
                    non_preferred_names=Convert_str_list(non_preferred_names)
                    if "None None" in non_preferred_names:
                        non_preferred_names.remove("None None")
                    pattern = r'[' + string.punctuation + ']'
                    non_pref_names=""
                    search_results_1=[]
                    for non_names in non_preferred_names:
                        non_pref_names=non_names
                        non_pref_names = re.sub(pattern, ' ', str(non_pref_names))
                        non_pref_names= ' '.join(non_pref_names.split())
                        non_pref_names=Convert(non_pref_names)
                        non_pref_output = sum([list(map(list, combinations(non_pref_names, i))) for i in range(len(non_pref_names) + 1)], [])
                        search_results=[]
                        search_result=[]
                        for names in non_pref_output[1:]:
                            search_terms=" ".join(map(str, names))
                            path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                            name="search_terms"
                            s = pd.Series(data=path1, name=name, dtype=str)
                            search_results.append(s)
                        df_results_non_preferred_names=pd.concat(search_results, ignore_index=True)
                        df_results_non_preferred_names=pd.DataFrame(df_results_non_preferred_names, columns=['search_terms']).reset_index()
                        m= pd.Series(data=df_results_non_preferred_names.search_terms, dtype=str)
                        search_results_1.append(m)
                    df_results_non_preferred_names = pd.concat(search_results_1, axis=0)
                    df_results_non_preferred_names=pd.DataFrame(df_results_non_preferred_names, columns=['search_terms']).reset_index()
                    df_results_non_preferred_names = df_results_non_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                else:
                    pass

                pattern = r'[' + string.punctuation + ']'
                surname_name = ""

                forename = re.sub(pattern, ' ', str(forename))
                surname = re.sub(pattern, ' ', str(surname))

                forename = ' '.join(forename.split())
                forename =forename.strip()

                surname = ' '.join(surname.split())
                surname =surname.strip()

                name_addition = re.sub(pattern, ' ', str(name_addition))
                name_addition =name_addition.strip()
                name_addition = ' '.join(name_addition.split())
                name_addition = name_addition.replace('  ', " ")

                personal_name = re.sub(pattern, ' ', str(personal_name))
                personal_name = ' '.join(personal_name.split())
                personal_name =personal_name.strip()

                prefix = re.sub(pattern, ' ', str(prefix))
                prefix = ' '.join(prefix.split())
                prefix =prefix.strip()

                forename = forename.replace('  ', " ")
                forename = forename.replace('von', ' ')
                forename = forename.replace(' und ', ' ')
                forename = forename.replace(' bei ', ' ')
                forename = forename.replace(' auf ', ' ')
                forename = forename.replace(' des ', ' ')
                forename = forename.replace(' zu ', ' ')
                forename = forename.replace(' and ', ' ')
                forename = forename.replace(' zum ', ' ')
                forename = ' '.join(forename.split())

                surname = surname.replace('  ', " ") 
                surname = surname.replace('von', ' ')
                surname = surname.replace(' und ', ' ')
                surname = surname.replace(' bei ', ' ')
                surname = surname.replace(' auf ', ' ')
                surname = surname.replace(' des ', ' ')
                surname = surname.replace(' zu ', ' ')
                surname = surname.replace(' and ', ' ')
                surname = surname.replace(' zum ', ' ')
                surname = ' '.join(surname.split())
                
                name_addition = name_addition.replace('  ', " ")
                name_addition = name_addition.replace(',', "")
                name_addition = name_addition.replace(' von  ', ' ')
                name_addition = name_addition.replace(' und ', ' ')
                name_addition = name_addition.replace(' bei ', ' ')
                name_addition = name_addition.replace(' auf ', ' ')
                name_addition = name_addition.replace(' des ', ' ')
                name_addition = name_addition.replace(' zu ', ' ')
                name_addition = name_addition.replace(' and ', ' ')
                name_addition = name_addition.replace(' zum ', ' ')
                name_addition = ' '.join(name_addition.split())

                personal_name = personal_name.replace('  ', " ")
                personal_name = personal_name.replace(' von ', ' ')
                personal_name = personal_name.replace(' und ', ' ')
                personal_name = personal_name.replace(' bei ', ' ')
                personal_name = personal_name.replace(' auf ', ' ')
                personal_name = personal_name.replace(' des ', ' ')
                personal_name = personal_name.replace(' zu ', ' ')
                personal_name = personal_name.replace(' and ', ' ')
                personal_name = personal_name.replace(' zum ', ' ')
                personal_name = ' '.join(personal_name.split())

                prefix = prefix.replace('  ', " ")
                prefix = prefix.replace(' von ', ' ')
                prefix = prefix.replace(' und ', ' ')
                prefix = prefix.replace(' bei ', ' ')
                prefix = prefix.replace(' auf ', ' ')
                prefix = prefix.replace(' des ', ' ')
                prefix = prefix.replace(' zu ', ' ')
                prefix = prefix.replace(' and ', ' ')
                prefix = prefix.replace(' zum ', ' ')
                prefix = ' '.join(prefix.split())

                if forename !="None" and surname !="None" and personal_name !="None" and name_addition !="None" and prefix !="None":
                    surname_name = forename + " " + surname
                    search_terms = forename+ " " +surname+ " "+ personal_name+ " "+name_addition+ " "+ prefix

                elif forename !="None" and surname !="None" and personal_name !="None" and name_addition !="None" and prefix =="None":
                    surname_name = forename + " " + surname
                    search_terms = forename+ " " +surname+ " "+ personal_name+ " "+name_addition

                elif forename !="None" and surname !="None" and personal_name !="None" and name_addition =="None" and prefix =="None":
                    surname_name = forename + " " + surname
                    search_terms = forename+ " " +surname+ " "+ personal_name

                elif forename !="None" and surname !="None" and personal_name =="None" and name_addition =="None" and prefix =="None":
                    surname_name = forename + " " + surname
                    search_terms = forename+ " " +surname

                elif forename !="None" and surname =="None" and personal_name =="None" and name_addition =="None" and prefix =="None":
                    surname_name = forename
                    search_terms = forename

                elif forename =="None" and surname !="None" and personal_name =="None" and name_addition =="None" and prefix =="None":
                    surname_name = surname
                    search_terms = surname

                elif forename =="None" and surname =="None" and personal_name !="None" and name_addition =="None" and prefix =="None":
                    surname_name = personal_name
                    search_terms = personal_name

                elif forename =="None" and surname =="None" and personal_name =="None" and name_addition !="None" and prefix =="None":
                    surname_name = name_addition
                    search_terms = name_addition

                elif forename =="None" and surname =="None" and personal_name =="None" and name_addition =="None" and prefix !="None":
                    surname_name = prefix
                    search_terms = prefix

                elif forename =="None" and surname =="None" and personal_name !="None" and name_addition !="None" and prefix =="None":
                    surname_name = personal_name+" "+name_addition
                    search_terms = personal_name+" "+name_addition

                elif forename =="None" and surname =="None" and personal_name !="None" and name_addition =="None" and prefix !="None":
                    surname_name = personal_name + " " + prefix
                    search_terms = personal_name + " " + prefix

                elif forename !="None" and surname !="None" and personal_name =="None" and name_addition !="None" and prefix =="None":
                    surname_name = forename + " " + surname
                    search_terms = forename + " " + surname+" "+name_addition

                elif forename !="None" and surname !="None" and personal_name !="None" and name_addition =="None" and prefix =="None":
                    surname_name = forename + " " + surname
                    search_terms = forename + " " + surname+" "+personal_name

                elif forename !="None" and surname !="None" and personal_name =="None" and name_addition =="None" and prefix !="None":
                    surname_name = forename + " " + surname
                    search_terms = forename + " " + surname+" "+prefix

                elif forename =="None" and surname =="None" and personal_name !="None" and name_addition !="None" and prefix !="None":
                    surname_name = personal_name + " " + name_addition + " " + prefix
                    search_terms = personal_name + " " + name_addition + " " + prefix

                sample_converted=Convert(search_terms)
                sample_converted = [num for num in sample_converted if len(num) > 1 and len(num) < 15]
                number_of_words_1 = [len(sentence.split()) for sentence in sample_converted]
                number_of_words_1 =sum(number_of_words_1)
                np_lists = np.array(sample_converted)

                if non_preferred_names !="None" or non_preferred_names !="None None":
                    source_non_preferred_names= ''.join(str(i) for i in non_preferred_names)
                else:
                    pass

                if birthdate != "None":
                    birthdate = re.findall('(\d{4})', str(birthdate))
                    birthdate = re.sub(pattern, '', str(birthdate))
                    birthdate_lenght = len(birthdate)
                    if birthdate != '9999' and birthdate_lenght==4:
                        path2 = str(birthdate) + "*"
                        birthdate=birthdate
                    else:
                        path2 = "9999*"
                        birthdate="9999"

                else:
                    birthdate = re.sub(pattern, '', str(birthdate))
                    path2 = birthdate.replace('None', "9999*")
                    birthdate = birthdate.replace('None', "9999")

                if deathdate != "None":
                    deathdate = re.findall('(\d{4})', str(deathdate))
                    deathdate = re.sub(pattern, '', str(deathdate))
                    deathdate_lenght = len(deathdate)
                    if deathdate != '9999' and deathdate_lenght==4:
                        path3 = str(deathdate) + "*"
                        deathdate=deathdate
                    else:
                        path3 = "9999*"
                        deathdate="9999"
                    
                else:
                    deathdate = re.sub(pattern, '', str(deathdate))
                    path3 = deathdate.replace('None', "9999*")
                    deathdate = deathdate.replace('None', "9999")

                if so_placeOfBirth != "None":
                    birthplace = so_placeOfBirth
                else:
                    birthplace ='Not Described'

                if so_placeOfDeath != "None":
                    deathplace = so_placeOfDeath
                else:
                    deathplace ='Not Described'

                if so_profession != "None":
                    profession = so_profession
                else:
                    profession = 'No Profession'

                if gender != "None":
                    gender = gender
                else:
                    gender = 'Not described'

                if so_period_of_activities_start != "None":
                    so_period_of_activities_start = re.findall('(\d{4})', str(so_period_of_activities_start))
                    pattern = r'[' + string.punctuation + ']'
                    so_period_of_activities_start = re.sub(pattern, '', str(so_period_of_activities_start))
                    so_period_of_activities_start_lenght = len(so_period_of_activities_start)
                    if so_period_of_activities_start_lenght==4:
                        so_period_of_activities_start = str(so_period_of_activities_start)
                    else:
                        so_period_of_activities_start = '9999'
                else:
                    so_period_of_activities_start = '9999'     

                if so_period_of_activities_end != "None":
                    so_period_of_activities_end = re.findall('(\d{4})', str(so_period_of_activities_end))
                    pattern = r'[' + string.punctuation + ']'
                    so_period_of_activities_end = re.sub(pattern, '', str(so_period_of_activities_end))
                    so_period_of_activities_end_lenght = len(so_period_of_activities_end)
                    if so_period_of_activities_end_lenght==4:
                        so_period_of_activities_end = str(so_period_of_activities_end)
                    else:
                        so_period_of_activities_end = '9999'
                else:
                    so_period_of_activities_end = '9999'

                info_table = [{'index_number':index_id, 'dc_id':dc_id, 'person_id':person_id,'data_history':'be added', 'source_system': data_id, 'entity_type': 'Person', 
                'surname': surname_org, 'forename': forename_org, 'personal_name':personal_name_org, 'name_addition':name_addition_org,'counting':counting_org, 
                'prefix':prefix_org,'non_preferred_name':non_preferred_names_org,'preferred':'preferred','gender':gender_org, 'birthdate':birthdate_org,
                'deathdate':deathdate_org,'birthplace':so_placeOfBirth_org,'deathplace':so_placeOfDeath_org,'profession':so_profession_org, 'descriptions':'so_description'}]
                NDS_Id =str(dc_id)

                index_number=str(index_id)
                info_table = pd.DataFrame(info_table, columns=['dc_id','person_id','data_history','source_system','entity_type','surname','forename', 'personal_name','name_addition','counting','prefix','non_preferred_name','preferred','gender','birthdate','deathdate','birthplace','deathplace','profession', 'descriptions'])
                info_table['data_history'] = info_table['data_history'].apply(lambda x: f'<a href="{x}">Url</a>')
                info_table = info_table.transpose()
                info_table.reset_index(drop=True)
                

                df_birthdate = pd.Series(data=path2, name='birthdate', dtype=str)
                df_birthdate = pd.DataFrame(df_birthdate, columns=['birthdate'])

                df_deathdate = pd.Series(data=path3, name='deathdate', dtype=str)
                df_deathdate = pd.DataFrame(df_deathdate, columns=['deathdate'])
                dates = pd.concat([df_birthdate,df_deathdate], axis=1)
            
                output=[]
                if number_of_words_1 == 1:
                    output = sample_converted
                elif number_of_words_1 > 1 and number_of_words_1 <= 7:
                    filt = []
                    for i in range(0,len(sample_converted)):
                        filt_i = len(np_lists[i]) >= 1
                        filt.append(filt_i)
                    new_list = list(np_lists[filt])
                    output = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
                elif number_of_words_1 > 7 :
                    filt = []
                    for i in range(0,len(sample_converted)):
                        filt_i = len(np_lists[i]) >= 1
                        filt.append(filt_i)
                    new_list = list(np_lists[filt])
                    combined_list = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
                    output = combined_list[0:20]
                else:
                    pass

                search_results=[]
                search_result=[]
                
                for names in output[0:]:
                    
                    search_terms=""
                    if number_of_words_1 == 1:
                        search_terms=str(names[0:])

                    elif number_of_words_1 > 1:
                        search_terms=" ".join(map(str, names))
                        search_terms=search_terms.strip()

                    number_of_words = len(search_terms.split())
                    if number_of_words_1 == 1 and number_of_words == 1:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        df_results_preferred_names=pd.DataFrame(s, columns=['search_terms'])
                    elif number_of_words_1 == 2 and number_of_words_1 > 1 and number_of_words > 1:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                    elif number_of_words_1 <= 4 and number_of_words_1 > 2 and number_of_words > 1:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                    elif number_of_words_1 >= 4 and number_of_words_1 < 6 and number_of_words >= 3:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                        
                    elif number_of_words_1 ==6 and number_of_words >= 3:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                    
                    elif number_of_words_1 ==7 and number_of_words >= 3:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)

                    elif number_of_words_1 > 10 and number_of_words_1 < 15 and number_of_words >=4:
                        path1=str(search_terms).replace(' ', lev_distance) + lev_distance
                        name="search_terms"
                        s = pd.Series(data=path1, name=name, dtype=str)
                        search_results.append(s)
                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                df_results_preferred_names=pd.DataFrame(df_results_preferred_names, columns=['search_terms'])
                df_results = df_results_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                
                df_results=pd.concat([df_results, dates], axis=1)
                df_results['birthdate']=df_results['birthdate'].fillna(method='ffill')
                df_results['deathdate']=df_results['deathdate'].fillna(method='ffill')

                name_surname=""
                data_results = []
                merged_queries = {'queries': {}}
                for term in df_results.values.tolist():
                    name_surname= str(term[0])
                    so_birthdate= str(term[1])
                    so_deathdate= str(term[2])
                    if so_birthdate=="9999*":
                        so_birthdate="NoDate"
                    else:
                        so_birthdate=so_birthdate
                    if so_deathdate=="9999*":
                        so_deathdate="NoDate"
                    else:
                        so_deathdate=so_deathdate
                    if so_birthdate != "NoDate" and so_deathdate != "NoDate":
                        data = {'queries': 
                                '{"q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson", "limit":"'+limit_size+'"}, \
                                "q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson","properties":[{"pid":"dateOfBirth","v":"'+so_birthdate+'"}], "limit":"'+limit_size+'"}, \
                                "q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson","properties":[{"pid":"dateOfDeath","v":"'+so_deathdate+'"}], "limit":"'+limit_size+'"}, \
                                "q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson","properties":[{"pid":"dateOfBirth","v":"'+so_birthdate+'"}, {"pid":"dateOfDeath","v":"'+so_deathdate+'"}], "limit":"'+limit_size+'"}}'}

                    elif so_birthdate!="NoDate" and so_deathdate=="NoDate":
                        data = {'queries': 
                                '{"q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson", "limit":"'+limit_size+'"}, \
                                "q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson","properties":[{"pid":"dateOfBirth","v":"'+so_birthdate+'"}], "limit":"'+limit_size+'"}}'}

                    elif so_birthdate=="NoDate" and so_deathdate!="NoDate":
                        data = {'queries': 
                                '{"q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson", "limit":"'+limit_size+'"}, \
                                "q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson","properties":[{"pid":"dateOfDeath","v":"'+so_deathdate+'"}], "limit":"'+limit_size+'"}}'}

                    elif so_birthdate=="NoDate" and so_deathdate=="NoDate":
                        data = {'queries': 
                                '{"q_":{"query":"'+name_surname+'","type":"DifferentiatedPerson", "limit":"'+limit_size+'"}}'} 
                    name="data"
                    s = pd.Series(data=data, name=name, dtype=str)
                    data_results.append(s)
      
                df_results=pd.concat(data_results, ignore_index=True, axis=0)

                data_query_results = []

                for my_string in df_results.values.tolist():
                    
                    merged_string=my_string.replace("}, {", ", ")
                    button_number_count = 1;
                    counter = count(button_number_count)
                    merged_string = re.sub(r'q_', lambda x:x.group(0) + str(next(counter)), merged_string)
                    merged_string = remove_extra_spaces(merged_string)
                    merged_queries = {'queries': merged_string}
                    query_id_result=pd.DataFrame.from_dict([merged_queries])
                    url = 'https://lobid.org/gnd/reconcile/'
                    encoded_data = urllib.parse.urlencode(merged_queries).encode('utf-8')

                    time.sleep(1)
                    request = urllib.request.Request(url,encoded_data)
                    response = json.load(urllib.request.urlopen(request))
                    result=pd.json_normalize(response)

                    id_list = []
                    clean_list = []
                    for query in response:                    
                        result_list = response[query]['result']
                        for result in result_list:
                            id_value = result['id']
                            id_list.append(id_value)
                                
                    clean_list = list(set(id_list))
                    clean_list_string = ', '.join(['"{0}"'.format(x) for x in clean_list])

                    url = 'https://lobid.org/gnd/reconcile/'
                    data_1 = {'extend':'{"ids":[' + clean_list_string + '],"properties":[\
                    {"id":"id"},\
                    {"id":"preferredName"},\
                    {"id":"variantName"},\
                    {"id":"dateOfBirth"},\
                    {"id":"placeOfBirth"},\
                    {"id":"dateOfDeath"},\
                    {"id":"placeOfDeath"},\
                    {"id":"professionOrOccupation"}]}'}

                    # start_time = time.time()
                    encoded_data = urllib.parse.urlencode(data_1).encode('utf-8')
                    request = urllib.request.Request(url, encoded_data)
                    response = json.load(urllib.request.urlopen(request))

                    rows = response["rows"]

                    # Initialize empty lists to store individual data
                    ids = []
                    ids_url=[]
                    preferred_names = []
                    variant_names = []
                    dob = []
                    pob = []
                    dod = []
                    pod = []
                    professions = []

                    # Loop through each row and extract data
                    for row_id, row_data in rows.items():
                        ids.append(row_id)
                        ids_url.append(row_data["id"][0]["str"])
                        preferred_names.append(row_data["preferredName"][0]["str"] if row_data.get("preferredName") else "NopreferredName")
                        variant_data = row_data.get("variantName")
                        if variant_data:
                            variant_name_list = [item["str"] for item in variant_data if item.get("str")]
                            variant_name_list = [item["str"].replace(',', '') for item in variant_data if item.get("str")]
                            if variant_name_list:
                                variant_names.append(", ".join(variant_name_list))
                            else:
                                variant_names.append('NoVariantName')
                        else:
                            variant_names.append('NoVariantName')

                        dob_data = row_data.get("dateOfBirth")
                        if dob_data:
                            dob.append(dob_data[0].get("str"))
                        else:
                            dob.append('NoDateOfBirth')

                        pob_data = row_data.get("placeOfBirth")
                        if pob_data:
                            pob.append(pob_data[0].get("name"))
                        else:
                            pob.append('NoPlaceOfBirth')
                        
                        dod_data = row_data.get("dateOfDeath")
                        if dod_data:
                            dod.append(dod_data[0].get("str"))
                        else:
                            dod.append('NoDateOfDeath')
                        
                        pod_data = row_data.get("placeOfDeath")
                        if pod_data:
                            pod.append(pod_data[0].get("name"))
                        else:
                            pod.append('NoPlaceOfDeath')
                        
                        prof_data = row_data.get("professionOrOccupation")
                        if prof_data:
                            profession_names = [item.get("name") for item in prof_data if item.get("name")]
                            if profession_names:
                                professions.append(", ".join(profession_names))
                            else:
                                professions.append('NoProfession')
                        else:
                            professions.append('NoProfession')

                    # Create a pandas DataFrame
                    data = {
                        "gndIdentifier": ids,
                        "id":ids_url,
                        "preferredName": preferred_names,
                        "variantName": variant_names,
                        "dateOfBirth": dob,
                        "placeOfBirth": pob,
                        "dateOfDeath": dod,
                        "placeOfDeath": pod,
                        "professionOrOccupation": professions,
                    }
                    s = pd.DataFrame(data)
                    data_query_results.append(s)
      
                df=pd.concat(data_query_results, ignore_index=True, axis=0)
                df=df.drop_duplicates(subset=['gndIdentifier'],keep='first')       

                if not df.empty and 'gndIdentifier' in df and 'preferredName' in df and 'variantName' in df and 'dateOfBirth' in df and 'placeOfBirth' in df and 'dateOfDeath' in df and 'placeOfDeath' in df and 'professionOrOccupation' in df:              
                    lev_threshold = 2
                    metaphone_threshold = 90
                    year_threshold = 5
                    check_params_name=2
                    check_params_profession = 1
                
                    #this is from private dataset
                    #preferred names

                    so_preferredName = [{'source_preferredName': surname_name}]
                    so_preferredName=pd.DataFrame(so_preferredName, columns=['source_preferredName'])
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].astype(str)
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].map(lambda x: x.lower())
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].map(lambda x: x.strip())
                    # so_preferredName = so_preferredName.apply(lambda x: x.replace({' van ':'', ' el ':'', 'von':'', ' al ':'', ' ibn ':'', ' abd ':'', ' und ':''}, regex=True))
                    # so_preferredName['source_preferredName'] = so_preferredName['source_preferredName'].str.replace('[{}]'.format(string.punctuation), ' ')

                    #non-preferred names
                    so_nonpreferredName = [{'source_nonpreferredName': source_non_preferred_names}]
                    so_nonpreferredName=pd.DataFrame(so_nonpreferredName, columns=['source_nonpreferredName'])
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].astype(str)
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].map(lambda x: x.lower())
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].map(lambda x: x.strip())

                    #this is from GND (preferredName)
                    des_preferredName = pd.DataFrame(df, columns=['gndIdentifier','preferredName'])
                    des_preferredName_org = pd.DataFrame(des_preferredName, columns=['preferredName'])
                    des_preferredName = pd.DataFrame(des_preferredName, columns=['preferredName'])
                    des_preferredName['dest_preferredName'] = [" ".join(n.split(", ")[::-1]) for n in df['preferredName']]

                    des_preferredName = pd.DataFrame (des_preferredName, columns = ['dest_preferredName'])
                    #des_preferredName = des_preferredName.apply(lambda x: x.replace({' van ':'', ' el ':'', 'von':'', ' al ':'', ' ibn ':'', ' abd ':'', ' und ':' '}, regex=True))
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].map(lambda x: x.lower())
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].map(lambda x: x.strip())
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)                     
                    preferredName_table = pd.concat([so_preferredName, des_preferredName],axis=1)
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].fillna(value=surname_name)

                    preferredName_table['check_params_preferred_name'] = np.where((preferredName_table['source_preferredName'] != type(None)) & (preferredName_table['dest_preferredName'] != type(None)), check_params_name, 0)
                    
                    non_preferredName_table = pd.concat([so_nonpreferredName, des_preferredName],axis=1)
                    non_preferredName_table['source_nonpreferredName'] = non_preferredName_table['source_nonpreferredName'].fillna(value=source_non_preferred_names)

                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].map(lambda x: x.lower())
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].map(lambda x: x.strip())
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                    preferredName_table.reset_index(inplace=True, drop=True)
                    ddf = dd.from_pandas(preferredName_table, npartitions=2)
                    result_preferredName= ddf.compute()
                    so_preferredName =so_preferredName.source_preferredName.str.split(expand=True)
                    so_nonpreferredName=so_nonpreferredName.source_nonpreferredName.str.split(expand=True)

                    so_preferredName = so_preferredName.values.tolist()
                    so_nonpreferredName=so_nonpreferredName.values.tolist()

                    des_preferredName = pd.DataFrame(df, columns=['preferredName'])
                    des_preferredName_id = pd.DataFrame(df, columns=['gndIdentifier'])
                    des_preferredName_url = pd.DataFrame(df, columns=['id'])
                    des_preferredName['dest_preferredName'] = [" ".join(n.split(", ")[::-1]) for n in des_preferredName['preferredName']]
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].astype(str)
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].map(lambda x: x.lower())
                    des_preferredName['dest_preferredName'] = des_preferredName['dest_preferredName'].map(lambda x: x.strip())
                    des_preferredName=des_preferredName['dest_preferredName'].str.split(expand=True)

                    des_preferredName = des_preferredName.values.tolist()

                    #this is for pref-name versus pref-name
                    pref_name_score=[]
                    pref_name_scores=[]
                    for items in des_preferredName:
                        filtered_list = list(filter(None, items))
                        list_1 = so_preferredName[0]
                        list_2 = filtered_list
                        score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
                        check_abbreviation_param = []
                        for (items_1, items_2) in zip(list_1, list_2) :
                            for i in range(0,len(list_1)):
                                for j in range(0,len(list_2)):
                                    size_1 = len(items_1)
                                    size_2 = len(items_2)
                                    if size_1 < 3 and size_2 < 3 and items_1 != items_2:
                                        check_abbreviation_param = 1
                                    score_matrix[i,j] = distance(list_1[i], list_2[j])
                        if 0 in score_matrix and 1 not in score_matrix:
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            pref_name_score= result1
                        elif 0 in score_matrix and 1 in score_matrix:
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                pref_name_score= result1 + result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                pref_name_score= result1 + result3
                        elif 1 in score_matrix and check_abbreviation_param != 1:
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                pref_name_score= result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                pref_name_score= result3
                            else:
                                result4=0
                                pref_name_score=result4                         
                        else:
                            pref_name_score = 0
                        name='pref_name_score'
                        s = pd.Series(data=pref_name_score, name=name, dtype=float)
                        pref_name_scores.append(s)
                    pref_name_score_table = pd.concat(pref_name_scores, ignore_index=True)
                    score_table_pref_names=pd.concat([result_preferredName,pref_name_score_table],axis=1)

                    #this is for non-pref-name versus pref-name
                    non_pref_name_score=[]
                    non_pref_name_scores=[]
                    for items in des_preferredName:
                        filtered_list = list(filter(None, items))
                        list_0 = so_nonpreferredName[0]
                        list_1 = list(set(list_0))
                        list_2 = filtered_list
                        score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
                        check_abbreviation_param = []
                        for (items_1, items_2) in zip(list_1, list_2) :
                            for i in range(0,len(list_1)):
                                for j in range(0,len(list_2)):
                                    size_1 = len(items_1)
                                    size_2 = len(items_2)
                                    if size_1 < 3 and size_2 < 3 and items_1 != items_2:
                                        check_abbreviation_param = 1
                                    score_matrix[i,j] = distance(list_1[i], list_2[j])  
                        if 0 in score_matrix and 1 not in score_matrix and list_1[0] != "nan":
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            non_pref_name_score= result1
                        elif 0 in score_matrix and 1 in score_matrix and list_1[0] != "nan":
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                non_pref_name_score= result1 + result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                non_pref_name_score= result1 + result3
                        elif 1 in score_matrix and check_abbreviation_param != 1 and list_1[0] != "nan":
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                non_pref_name_score= result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                non_pref_name_score= result3
                            else:
                                result4=0
                                non_pref_name_score=result4                         
                        else:
                            non_pref_name_score = 0
                        name='non_pref_name_score'
                        s = pd.Series(data=non_pref_name_score, name=name, dtype=float)
                        non_pref_name_scores.append(s)
                    non_pref_name_score_table = pd.concat(non_pref_name_scores, ignore_index=True)
                    score_table_non_pref_names=pd.concat([non_preferredName_table,non_pref_name_score_table],axis=1)

                    #this is from GND(variantName)

                    des_variant_name = pd.DataFrame(df, columns=['variantName'])
                    des_variant_name_org = pd.DataFrame(df, columns=['variantName'])
                
                    des_variant_name['des_variantName'] = des_variant_name['variantName'].map(lambda x: x.lower())
                    des_variant_name['des_variantName'] = des_variant_name['des_variantName'].map(lambda x: x.strip())

                    so_preferredName = [{'source_preferredName': surname_name}]
                    so_preferredName=pd.DataFrame(so_preferredName, columns=['source_preferredName'])
                    
                    variantName_table = pd.concat([so_preferredName, des_variant_name],axis=1)
                    variantName_table = variantName_table.fillna(value=surname_name)
                    variantName_table['source_preferredName'] = variantName_table['source_preferredName'].map(lambda x: x.lower())
                    variantName_table['source_preferredName'] = variantName_table['source_preferredName'].map(lambda x: x.strip())

                    so_preferredName_df = [{'source_preferredName': surname_name}]
                    so_preferredName_df=pd.DataFrame(so_preferredName_df, columns=['source_preferredName'])
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].astype(str)
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].map(lambda x: x.lower())
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].map(lambda x: x.strip()) 
                    so_preferredName_df =so_preferredName_df.source_preferredName.str.split(expand=True)
                    
                    so_preferredName_df = so_preferredName_df.values.tolist()

                    des_variant_name_explode=des_variant_name['des_variantName']
                    des_variant_name['des_variantName']=des_variant_name['des_variantName'].str.split(',')

                    des_variant_name_explode=des_variant_name.explode('des_variantName')
                    
                    des_variant_name_explode_df=des_variant_name_explode.reset_index()
                    
                    des_variant_name_explode=des_variant_name_explode['des_variantName'].values.tolist()

                    #this is for pref-name versus var-name
                    var_name_score=[]
                    var_name_scores=[]

                    for items in des_variant_name_explode:
                        filtered_list = items.split(' ')
                        filtered_list = list(filter(None, filtered_list))
                        list_1 = so_preferredName_df[0]
                        list_2 = list(set(filtered_list))
                        list_2 = filtered_list
                        score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
                        check_abbreviation_param = []
                        for (items_1, items_2) in zip(list_1, list_2) :
                            items_2=re.sub(r'[^\w\s]', '', items_2)
                            for i in range(0,len(list_1)):
                                for j in range(0,len(list_2)):
                                    size_1 = len(items_1)
                                    size_2 = len(items_2)
                                    if size_1 < 3 and size_2 < 3 and items_1 != items_2:
                                        check_abbreviation_param = 1
                                    score_matrix[i,j] = distance(list_1[i], list_2[j])  
                        if 0 in score_matrix and 1 not in score_matrix:
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            var_name_score= result1
                        elif 0 in score_matrix and 1 in score_matrix:
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                var_name_score= result1 + result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                var_name_score= result1 + result3
                        elif 1 in score_matrix and check_abbreviation_param != 1:
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                var_name_score= result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                var_name_score= result3
                            else:
                                result4=0
                                var_name_score=result4                         
                        else:
                            var_name_score = 0
                        name='var_name_score'
                        s = pd.Series(data=var_name_score, name=name, dtype=float)
                        var_name_scores.append(s)
                    var_name_score_table= pd.concat(var_name_scores, ignore_index=True)
                    var_name_score_table = pd.DataFrame(var_name_score_table, index=des_variant_name_explode_df.index.copy())
                    score_table_pref_variant_names=pd.concat([des_variant_name_explode_df,var_name_score_table],axis=1, ignore_index=False)

                    score_table_pref_variant_names=score_table_pref_variant_names.reset_index()
                    score_table_pref_variant_names_max = score_table_pref_variant_names.groupby(['index'])['var_name_score'].max()
                    score_table_pref_variant_names_max =pd.DataFrame(score_table_pref_variant_names_max, columns=["var_name_score"])

                    #this is for non-pref-name versus var-name
                    non_pref_var_name_score=[]
                    non_pref_var_name_scores=[]

                    for items in des_variant_name_explode:
                        filtered_list = items.split(' ')
                        filtered_list = list(filter(None, filtered_list))
                        list_1 = so_nonpreferredName[0]
                        list_2 = list(set(filtered_list))
                        list_2 = filtered_list                            
                        score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
                        check_abbreviation_param = []
                        for (items_1, items_2) in zip(list_1, list_2) :
                            items_2=re.sub(r'[^\w\s]', '', items_2)
                            for i in range(0,len(list_1)):
                                for j in range(0,len(list_2)):
                                    size_1 = len(items_1)
                                    size_2 = len(items_2)
                                    if size_1 < 3 and size_2 < 3 and items_1 != items_2:
                                        check_abbreviation_param = 1
                                    score_matrix[i,j] = distance(list_1[i], list_2[j])  
                        if 0 in score_matrix and 1 not in score_matrix and list_1[0] != "nan":
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            non_pref_var_name_score= result1
                        elif 0 in score_matrix and 1 in score_matrix and list_1[0] != "nan":
                            count_0=(score_matrix==0).sum()
                            result1 = count_0
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                non_pref_var_name_score= result1 + result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                non_pref_var_name_score= result1 + result3
                        elif 1 in score_matrix and check_abbreviation_param != 1 and list_1[0] != "nan":
                            count_1=(score_matrix==1).sum()
                            if count_1 == 1:
                                result2=0.9
                                non_pref_var_name_score= result2
                            elif count_1 > 1:
                                result3=count_1*0.9
                                non_pref_var_name_score= result3
                            else:
                                result4=0
                                non_pref_var_name_score=result4                         
                        else:
                            non_pref_var_name_score = 0
                        name='non_pref_var_name_score'
                        s = pd.Series(data=non_pref_var_name_score, name=name, dtype=float)
                        non_pref_var_name_scores.append(s)
                    non_pref_var_name_score_table = pd.concat(non_pref_var_name_scores, ignore_index=True)

                    non_pref_var_name_score_table =pd.DataFrame(non_pref_var_name_score_table, index=des_variant_name_explode_df.index.copy())
                    score_table_nonpref_variant_names=pd.concat([non_preferredName_table['source_nonpreferredName'],des_variant_name_explode_df,non_pref_var_name_score_table],axis=1, ignore_index=False)
                    score_table_nonpref_variant_names=score_table_nonpref_variant_names.reset_index()
                    score_table_nonpref_variant_names_max = score_table_nonpref_variant_names.groupby(['index'])['non_pref_var_name_score'].max()
                    score_table_nonpref_variant_names_max =pd.DataFrame(score_table_nonpref_variant_names_max, columns=["non_pref_var_name_score"])


                    names_score_tables = pd.concat([score_table_pref_names, score_table_pref_variant_names_max["var_name_score"], score_table_non_pref_names['non_pref_name_score'],score_table_nonpref_variant_names_max['non_pref_var_name_score']],axis=1, ignore_index=False)
                    names_score_tables['max_name_score'] = names_score_tables[['pref_name_score','var_name_score', 'non_pref_name_score','non_pref_var_name_score']].max(axis=1)
                    max_name_score = names_score_tables['max_name_score'].max()
                    names_score_tables['IstMaxNameScore'] = np.where((names_score_tables['max_name_score']==max_name_score), 1, 0)
                    max_name_score_count = (names_score_tables.IstMaxNameScore.values == 1).sum()
                    names_score_tables['IstMx_x_Anz'] = np.where((names_score_tables['max_name_score']==max_name_score), max_name_score_count, 0)


                    #this is for birthdates
                    #from private dataset                    
                    if birthdate == 'NoDate':
                        birthdate = '9999'
                    else:
                        pass
                    so_dateOfBirth = [{'source_dateOfBirth': birthdate}]
                    so_dateOfBirth=pd.DataFrame(so_dateOfBirth, columns=['source_dateOfBirth'])

                    #from GND
                    des_birthdate = df['dateOfBirth']
                    des_birthdate_org = df['dateOfBirth']
                    des_birthdate = pd.DataFrame(des_birthdate, columns=['dateOfBirth'])

                    birthdate_data = []
                    for row in des_birthdate.values:
                        des_dateOfBirth = str(row[0])
                        if des_dateOfBirth == 'NoDateOfBirth':
                            des_dateOfBirth = '9999'
                        else:
                            des_dateOfBirth =des_dateOfBirth
                        date = []
                        date_info = [{"dateOfBirth":des_dateOfBirth}]
                        date_info_1 = pd.DataFrame(date_info)
                        birthdate_data.append(date_info_1)
                    birthdate_data=pd.concat(birthdate_data, ignore_index=True)
                    des_birthdate = pd.DataFrame(birthdate_data, columns=['dateOfBirth'])
                    des_birthdate['dateOfBirth'] =des_birthdate['dateOfBirth'].str.findall('(\d{4})').astype(str)
                    des_birthdate['dateOfBirth'] = [''.join(map(str, l)) for l in des_birthdate['dateOfBirth']]
                    des_birthdate['dateOfBirth'] =des_birthdate['dateOfBirth'].str.replace("[", '')
                    des_birthdate['dateOfBirth'] =des_birthdate['dateOfBirth'].str.replace("]", '')
                    des_birthdate['dateOfBirth'] =des_birthdate['dateOfBirth'].str.replace("'", '')
                    des_birthdate_data = pd.DataFrame(des_birthdate, columns=['dateOfBirth'])
                    des_birthdate_data=des_birthdate_data.rename({'dateOfBirth':'des_year_birth'}, axis=1)
                    des_birthdate_data['dateOfBirth_lenght']= des_birthdate_data['des_year_birth'].str.len()
                    des_birthdate_data['des_year_birth']=np.where((des_birthdate_data['dateOfBirth_lenght'] == 4),des_birthdate_data['des_year_birth'], '9999')

                    birthdates_table = pd.concat([so_dateOfBirth, des_birthdate_data['des_year_birth']],axis=1)
                    birthdates_table['source_dateOfBirth'] = [birthdate if x is np.nan else x for x in birthdates_table['source_dateOfBirth'] ]

                    birthdates_table['source_dateOfBirth'] = birthdates_table['source_dateOfBirth'].astype('Int64').astype(int)

                    birthdates_table['delta'] = np.where((birthdates_table.source_dateOfBirth != 9999) & (birthdates_table.des_year_birth.astype(int) != 9999), (birthdates_table['des_year_birth'].astype(int) - birthdates_table['source_dateOfBirth'].astype(int)), 9999)
                    birthdates_table['delta'] = abs(birthdates_table['delta']).astype(int)
                    birthdates_table['source_dateOfBirth'] = birthdates_table['source_dateOfBirth'].astype('Int64').astype(str)
                    birthdates_table['des_year_birth'] = birthdates_table['des_year_birth'].astype('Int64').astype(str)
                    birthdates_table['Difference'] = np.where(birthdates_table['source_dateOfBirth'] == birthdates_table['des_year_birth'] , 1, 0)

                    birthdates_table['checked_params_birthdate_source'] = np.where((birthdates_table['source_dateOfBirth'] == "9999"), 0, 1)
                    birthdates_table['checked_params_birthdate_des'] = np.where((birthdates_table['des_year_birth'] == "9999"), 0, 1)
                    birthdates_table['checked_params_birthdate'] = np.where(((birthdates_table.checked_params_birthdate_source > 0) & (birthdates_table.checked_params_birthdate_des > 0)), 1, 0)
                    birthdate_score = []

                    for row in birthdates_table.values:
                        source_dateOfBirth = int(row[0])
                        des_year_birth=int(row[1])
                        delta=int(row[2])
                        score=[]
                        if source_dateOfBirth == 9999 or des_year_birth == 9999:
                            score = 0
                        elif delta != 9999 and delta < year_threshold:
                            score = 1-(delta/10)
                        else:
                            score = -1
                        score_info = [{"G_birthdate_score":score}]
                        score_info_1 = pd.DataFrame(score_info)
                        birthdate_score.append(score_info_1)

                    birthdate_score=pd.concat(birthdate_score, ignore_index=True)
                    G_score = pd.DataFrame(birthdate_score, columns=['G_birthdate_score'])

                    birthdate_score_table=pd.concat([birthdates_table['source_dateOfBirth'], birthdates_table['des_year_birth'], birthdates_table['delta'].astype(int),G_score['G_birthdate_score']], axis=1)

                    #this is for deathdates
                    #from private dataset
                    if deathdate == 'NoDate':
                        deathdate = '9999'
                    else:
                        pass
                    so_dateOfDeath = [{'source_dateOfDeath': deathdate}]
                    so_dateOfDeath=pd.DataFrame(so_dateOfDeath, columns=['source_dateOfDeath'])

                    #from GND
                    des_deathdate = df['dateOfDeath']
                    des_deathdate_org = df['dateOfDeath'] 
                    des_deathdate = pd.DataFrame(des_deathdate, columns=['dateOfDeath'])

                    deathdate_data = []
                    for row in des_deathdate.values:
                        des_dateOfDeath = str(row[0])
                        if des_dateOfDeath == 'NoDateOfDeath':
                            des_dateOfDeath = '9999'
                        else:
                            des_dateOfDeath =des_dateOfDeath
                        date = []
                        date_info = [{"dateOfDeath":des_dateOfDeath}]
                        date_info_1 = pd.DataFrame(date_info)
                        deathdate_data.append(date_info_1)

                    deathdate_data=pd.concat(deathdate_data, ignore_index=True)
                    des_deathdate = pd.DataFrame(deathdate_data, columns=['dateOfDeath'])
                    des_deathdate['dateOfDeath'] =des_deathdate['dateOfDeath'].str.findall('(\d{4})').astype(str)
                    des_deathdate['dateOfDeath'] = [''.join(map(str, l)) for l in des_deathdate['dateOfDeath']]
                    des_deathdate['dateOfDeath'] =des_deathdate['dateOfDeath'].str.replace("[", '')
                    des_deathdate['dateOfDeath'] =des_deathdate['dateOfDeath'].str.replace("]", '')
                    des_deathdate['dateOfDeath'] =des_deathdate['dateOfDeath'].str.replace("'", '')
                    des_deathdate_data = pd.DataFrame(des_deathdate, columns=['dateOfDeath'])
                    des_deathdate_data=des_deathdate_data.rename({'dateOfDeath':'des_year_death'}, axis=1)
                    des_deathdate_data['dateOfDeath_lenght']= des_deathdate_data['des_year_death'].str.len()
                    des_deathdate_data['des_year_death']=np.where((des_deathdate_data['dateOfDeath_lenght'] == 4),des_deathdate_data['des_year_death'], '9999')

                    deathdates_table = pd.concat([so_dateOfDeath, des_deathdate_data['des_year_death']],axis=1)
                    deathdates_table['source_dateOfDeath'] = [deathdate if x is np.nan else x for x in deathdates_table['source_dateOfDeath'] ]
                    deathdates_table['source_dateOfDeath'] = deathdates_table['source_dateOfDeath'].astype('Int64').astype(int)
                    deathdates_table['delta'] = np.where((deathdates_table.source_dateOfDeath != 9999) & (deathdates_table.des_year_death.astype(int) != 9999), (deathdates_table['des_year_death'].astype(int) - deathdates_table['source_dateOfDeath'].astype(int)), 9999)
                    deathdates_table['delta'] = abs(deathdates_table['delta']).astype(int)

                    deathdates_table['source_dateOfDeath'] = deathdates_table['source_dateOfDeath'].astype('Int64').astype(str)
                    deathdates_table['des_year_death'] = deathdates_table['des_year_death'].astype('Int64').astype(str)

                    deathdates_table['Difference'] = np.where(deathdates_table['source_dateOfDeath'] == deathdates_table['des_year_death'] , 1, 0)
                    deathdates_table['checked_params_deathdate_source'] = np.where((deathdates_table['source_dateOfDeath'] == "9999"), 0, 1)
                    deathdates_table['checked_params_deathdate_des'] = np.where((deathdates_table['des_year_death'] == "9999"), 0, 1)
                    deathdates_table['checked_params_deathdate'] = np.where(((deathdates_table.checked_params_deathdate_source > 0) & (deathdates_table.checked_params_deathdate_des > 0)), 1, 0)

                    deathdate_score = []
                    for row in deathdates_table.values:
                        source_dateOfDeath=int(row[0])
                        des_year_death=int(row[1])
                        delta=int(row[2])
                        score=[]
                        if source_dateOfDeath == 9999 or des_year_death == 9999:
                            score = 0
                        elif delta != 9999 and delta < year_threshold:
                            score = 1-(delta/10)
                        else:
                            score = -1
                        score_info = [{"H_deathdate_score":score}]
                        score_info_1 = pd.DataFrame(score_info)
                        deathdate_score.append(score_info_1)
                    deathdate_score=pd.concat(deathdate_score, ignore_index=True)
                    H_score = pd.DataFrame(deathdate_score, columns=['H_deathdate_score'])

                    deathdate_score_table=pd.concat([deathdates_table['source_dateOfDeath'], deathdates_table['des_year_death'], deathdates_table['delta'].astype(int), H_score['H_deathdate_score']], axis=1)
                    
                    #this is for birthplaces
                    #from private dataset
                    so_placeOfBirth = [{'source_placeOfBirth': birthplace}]
                    so_placeOfBirth=pd.DataFrame(so_placeOfBirth, columns=['source_placeOfBirth'])

                    #from GND
                    des_placeOfBirth = df['placeOfBirth']
                    des_placeOfBirth_org = df['placeOfBirth']
                    des_placeOfBirth = pd.DataFrame(des_placeOfBirth, columns=['placeOfBirth'])
                    des_placeOfBirth['placeOfBirth'] = [ 'NoPlaceOfBirth' if x is None else x for x in des_placeOfBirth['placeOfBirth'] ]
                    des_placeOfBirth['des_placeOfBirth'] = des_placeOfBirth['placeOfBirth']

                    birthplaces_table = pd.concat([so_placeOfBirth, des_placeOfBirth['placeOfBirth']],axis=1)
                    birthplaces_table['source_placeOfBirth'] = [birthplace if x is np.nan else x for x in birthplaces_table['source_placeOfBirth'] ]
                    birthplaces_table['source_placeOfBirth'] = birthplaces_table['source_placeOfBirth'].astype(str)
                    birthplaces_table['des_placeOfBirth'] = des_placeOfBirth['des_placeOfBirth'].astype(str)
                    result_birthplaces=birthplaces_table

                    result_birthplaces['checked_params_birthplace'] = np.where(((result_birthplaces.source_placeOfBirth != 'Not Described') & (result_birthplaces.des_placeOfBirth != 'No Birthplace')), '1', '0')

                    birthplace_score = []

                    for row in result_birthplaces.values:
                        source_placeOfBirth=str(row[0])
                        des_placeOfBirth=str(row[1])
                        score=[]
                        if source_placeOfBirth == 'Not Described' or des_placeOfBirth == 'NoPlaceOfBirth':
                            score = 0
                        elif source_placeOfBirth == des_placeOfBirth:
                            score = 1
                        elif source_placeOfBirth != des_placeOfBirth:
                            score = -1
                        score_info = [{"O_birthplace_score":score}]
                        score_info_1 = pd.DataFrame(score_info)
                        birthplace_score.append(score_info_1)
                    birthplace_score=pd.concat(birthplace_score, ignore_index=True)
                    O_score = pd.DataFrame(birthplace_score, columns=['O_birthplace_score'])
                    result_birthplaces = pd.concat([result_birthplaces, O_score], axis=1)                        

                    #this is for deathplaces
                    #from private dataset
                    so_placeOfDeath = [{'source_placeOfDeath': deathplace}]
                    so_placeOfDeath=pd.DataFrame(so_placeOfDeath, columns=['source_placeOfDeath'])

                    #from GND
                    des_placeOfDeath = df['placeOfDeath']
                    des_placeOfDeath_org = df['placeOfDeath']
                    des_placeOfDeath = pd.DataFrame(des_placeOfDeath, columns=['placeOfDeath'])

                    des_placeOfDeath.loc[des_placeOfDeath['placeOfDeath'].isnull(),'value_is_NaN'] = 'Yes'
                    des_placeOfDeath.loc[des_placeOfDeath['placeOfDeath'].notnull(), 'value_is_NaN'] = 'No'
                    des_placeOfDeath['placeOfDeath'] = np.where((des_placeOfDeath['value_is_NaN'] == "Yes"), None, des_placeOfDeath['placeOfDeath'])

                    des_placeOfDeath['placeOfDeath'] = [ 'NoPlaceOfDeath' if x is None else x for x in des_placeOfDeath['placeOfDeath'] ]
                    deathplaces_table = pd.concat([so_placeOfDeath, des_placeOfDeath['placeOfDeath']],axis=1)                            
                    deathplaces_table['source_placeOfDeath'] = [deathplace if x is np.nan else x for x in deathplaces_table['source_placeOfDeath'] ]
                    deathplaces_table['source_placeOfDeath'] = deathplaces_table['source_placeOfDeath'].astype(str)
                    deathplaces_table['des_placeOfDeath'] = des_placeOfDeath['placeOfDeath']
                    result_deathplaces=deathplaces_table
                    
                    result_deathplaces['checked_params_deathplace'] = np.where(((result_deathplaces.source_placeOfDeath != 'Not Described') & (result_deathplaces.des_placeOfDeath != 'No Deathplace')), '1', '0')

                    deathplace_score = []

                    for row in result_deathplaces.values:
                        source_placeOfDeath=str(row[0])
                        des_placeOfDeath=str(row[1])

                        score=[]
                        if source_placeOfDeath == 'Not Described' or des_placeOfDeath == 'NoPlaceOfDeath':
                            score = 0
                        elif source_placeOfDeath == des_placeOfDeath:
                            score = 1
                        elif source_placeOfDeath != des_placeOfDeath:
                            score = -1
                        score_info = [{"P_deathplace_score":score}]
                        score_info_1 = pd.DataFrame(score_info)
                        deathplace_score.append(score_info_1)
                    deathplace_score=pd.concat(deathplace_score, ignore_index=True)
                    P_score = pd.DataFrame(deathplace_score, columns=['P_deathplace_score'])
                    result_deathplaces = pd.concat([result_deathplaces, P_score], axis=1)     

                    #this is for profession
                    #from joblist
                    new_job_list= pd.read_csv('data/new_job_list2.csv', delimiter=';')
                    new_job_list = pd.DataFrame(new_job_list, columns=['name', 'code'])
                    
                    df_professions=new_job_list.sort_values(by='code', ascending=True)

                    #from NDS 
                    so_profession_org = [{'so_profession': profession}]
                    so_profession_org=pd.DataFrame(so_profession_org, columns=['so_profession']).astype(str)
                    so_profession_org = so_profession_org['so_profession'].str.split(',', expand=True)
                    so_profession_org_1 = pd.DataFrame(so_profession_org)
                    so_profession_org_1 =so_profession_org_1.values.tolist()
                    so_profession_org_1 =so_profession_org_1[0]
                    
                    counter = 0
                    new_list2=[]
                    for value in so_profession_org_1:
                        value=value.strip()                        
                        counter += 1
                        if value in df_professions.values:
                            row=df_professions.query("name == @value")
                            row=row.reset_index()
                            row = row.drop(row.index[np.where(row.index > 0)[0]])
                            new_list2.append(row)
                        elif value not in df_professions.values:
                            info=[{"name":"No Match", "code":"No Match"}]
                            info2=pd.DataFrame(info)
                            new_list2.append(info2)
                    job_list_match=pd.concat(new_list2,ignore_index=True)
                    job_list_match=job_list_match.rename({'code':'org_prof_code'}, axis=1)
                    job_list_match=job_list_match.rename({'name':'org_prof_name'}, axis=1)
                    
                    so_profession_org=pd.DataFrame(so_profession_org_1, columns=['so_profession']).astype(str)

                    so_profession_dc_id = [{'dc_id': dc_id}]
                    so_profession_dc_id =pd.DataFrame(so_profession_dc_id, columns=['dc_id'])
                    so_profession = pd.concat([so_profession_org, so_profession_dc_id, job_list_match], axis=1)
                    so_profession = pd.DataFrame(so_profession, columns=['dc_id','so_profession', 'org_prof_name','org_prof_code'])
                    so_profession['dc_id']=so_profession['dc_id'].fillna(method='ffill')
                    so_profession.reset_index(inplace=True, drop=True)
                    so_profession1 =so_profession
                                
                    #from GND
                    des_profession = df['professionOrOccupation']
                    des_profession_org = df['professionOrOccupation']
                    gnd_id = df['gndIdentifier']
                    des_profession = pd.DataFrame(des_profession, columns=['professionOrOccupation'])
                    des_profession['professionOrOccupation'] = ['NoProfession' if x is np.nan else x for x in des_profession['professionOrOccupation'] ]
                    professions_exploded=des_profession['professionOrOccupation']
                    des_profession['professionOrOccupation'] = des_profession['professionOrOccupation'].str.split(',')
                    professions_exploded=des_profession.explode('professionOrOccupation')
                    professions_exploded_df=professions_exploded.reset_index()
                    
                    new_list2=[]

                    for element in professions_exploded['professionOrOccupation'].values.tolist():
                        if element in df_professions.values:
                            sample_element=df_professions.query("name == @element")
                            sample_element=sample_element.reset_index()
                            sample_element = sample_element.drop(sample_element.index[np.where(sample_element.index > 0)[0]])
                            names=', '.join(sample_element.name)
                            codes=', '.join(sample_element.code)
                            names_codes=[{"name":names, "code":codes}]
                            names_codes=pd.DataFrame(names_codes)
                            new_list2.append(names_codes)
                        elif element not in df_professions.values:
                            info=[{"name":"No Match", "code":"No Match"}]
                            info2=pd.DataFrame(info)
                            new_list2.append(info2)
                    
                    job_list_match=pd.concat(new_list2,ignore_index=True)
                    job_list_match=job_list_match.rename({'code':'des_prof_code'}, axis=1)
                    job_list_match=job_list_match.rename({'name':'des_prof_name'}, axis=1)
                    job_list_match =pd.DataFrame(job_list_match,index=professions_exploded_df.index.copy())
                    des_prof_table=pd.concat([professions_exploded_df,job_list_match],axis=1, ignore_index=False)
                    result_profession_1 =pd.concat([so_profession1, des_prof_table], axis=1,ignore_index=False)
                    result_profession_1['check_params_profession'] = np.where(((result_profession_1.so_profession[0] != 'No Profession') & (result_profession_1.professionOrOccupation != 'No Profession')), check_params_profession, 0)
                    result_profession_1['so_prof_code_lenght'] = np.where((result_profession_1.org_prof_code != np.nan), result_profession_1['org_prof_code'].str.len(), 0.0)
                    result_profession_1['des_prof_code_lenght'] = np.where((result_profession_1.des_prof_code != 'No Match'), result_profession_1['des_prof_code'].str.len(), 0)


                    #for the same codes
                    matched_list= []
                    for value in result_profession_1['org_prof_code']:
                        value=str(value)
                        if value != 'nan':
                            value = value
                            result_profession_1['prof_score_1'] = np.where(((result_profession_1.org_prof_code[0] != np.nan) & (result_profession_1.org_prof_code[0] != "No Match") & (result_profession_1.check_params_profession != 0) & (value == result_profession_1['des_prof_code'])), 1, 0)
                            result_profession_score_1 = result_profession_1.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                            matched_list_1=result_profession_score_1['prof_score_1']
                            matched_list.append(matched_list_1)
                    matched_list=pd.concat(matched_list, axis=0)
                    matched_list = matched_list.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                    matched_list=pd.DataFrame(matched_list, columns=['prof_score_1'])
                    matched_list['max_job_score_1']= matched_list['prof_score_1'].apply(lambda x: max(map(float, x.split(','))))

                    #for the ones with more than 11 characters
                    matched_list_11= []
                    result_profession_1['sub_str_so_prof_code_11'] = np.where((result_profession_1['so_prof_code_lenght'] > 0), result_profession_1['org_prof_code'].astype(str).str.slice(start=0, stop=11), 'No Code')
                    result_profession_1['sub_str_des_prof_code_11'] = np.where(((result_profession_1['des_prof_code_lenght'] > 0)), result_profession_1['des_prof_code'].astype(str).str.slice(start=0, stop=11), 0)
                    for value in result_profession_1['sub_str_so_prof_code_11']:
                        value=str(value)
                        if value!="No Code":
                            value = value
                            result_profession_1['prof_score_11'] = np.where(((value != "No Code") & (value == result_profession_1['sub_str_des_prof_code_11'])), 1, 0)
                            result_profession_score_1 = result_profession_1.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                            matched_list_2=result_profession_score_1['prof_score_11']
                            matched_list_11.append(matched_list_2)
                    matched_list_11=pd.concat(matched_list_11, axis=0)
                    matched_list_11 = matched_list_11.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                    matched_list_11=pd.DataFrame(matched_list_11, columns=['prof_score_11'])
                    matched_list_11['max_job_score_11']= matched_list_11['prof_score_11'].apply(lambda x: max(map(float, x.split(','))))


                    #for the ones with more than 7 characters
                    matched_list_7= []
                    result_profession_1['sub_str_so_prof_code_7'] = np.where((result_profession_1['so_prof_code_lenght']  > 0), result_profession_1['org_prof_code'].astype(str).str.slice(start=0, stop=7), 'No Code')
                    result_profession_1['sub_str_des_prof_code_7'] = np.where(((result_profession_1['des_prof_code_lenght']  > 0)), result_profession_1['des_prof_code'].astype(str).str.slice(start=0, stop=7), 0)
                    for value in result_profession_1['sub_str_so_prof_code_7']:
                        value=str(value)
                        if value!="No Code":
                            value = value
                            result_profession_1['prof_score_7'] = np.where(((value != "No Code") & (value == result_profession_1['sub_str_des_prof_code_7'])), 0.75, 0)
                            result_profession_score_1 = result_profession_1.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                            matched_list_3=result_profession_score_1['prof_score_7']
                            matched_list_7.append(matched_list_3)
                    matched_list_7=pd.concat(matched_list_7, axis=0)
                    matched_list_7 = matched_list_7.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                    matched_list_7=pd.DataFrame(matched_list_7, columns=['prof_score_7'])
                    matched_list_7['max_job_score_7']= matched_list_7['prof_score_7'].apply(lambda x: max(map(float, x.split(','))))


                    #for the ones with more than 3 characters
                    matched_list_3= []
                    result_profession_1['sub_str_so_prof_code_3'] = np.where((result_profession_1['so_prof_code_lenght']  > 0), result_profession_1['org_prof_code'].astype(str).str.slice(start=0, stop=3), 'No Code')
                    result_profession_1['sub_str_des_prof_code_3'] = np.where(((result_profession_1['des_prof_code_lenght']  > 0)), result_profession_1['des_prof_code'].astype(str).str.slice(start=0, stop=3), 0)
                    for value in result_profession_1['sub_str_so_prof_code_3']:
                        value=str(value)
                        if value!="No Code":
                            value = value
                            result_profession_1['prof_score_3'] = np.where(((value != "No Code") & (value == result_profession_1['sub_str_des_prof_code_3'])), 0.25, 0)
                            result_profession_score_1 = result_profession_1.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                            matched_list_4=result_profession_score_1['prof_score_3']
                            matched_list_3.append(matched_list_4)
                    matched_list_3=pd.concat(matched_list_3, axis=0)
                    matched_list_3 = matched_list_3.groupby(['index']).agg(lambda col: ', '.join(map(str, col)))
                    matched_list_3=pd.DataFrame(matched_list_3, columns=['prof_score_3'])
                    matched_list_3['max_job_score_3']= matched_list_3['prof_score_3'].apply(lambda x: max(map(float, x.split(','))))

                    job_scores=pd.concat([matched_list['max_job_score_1'],matched_list_11['max_job_score_11'],matched_list_7['max_job_score_7'], matched_list_3['max_job_score_3']], axis=1)
                   
                    cols=['max_job_score_1', 'max_job_score_11', 'max_job_score_7', 'max_job_score_3']
                    job_scores['max'] = job_scores[cols].apply(lambda row: ', '.join(row.values.astype(str)), axis=1)
                    
                    job_scores['max_job_score']=job_scores['max'].apply(lambda x: max(map(float, x.split(','))))

                    #this is for Activity Period

                    #from NDS
                    source_period_of_activities_start = [{'source_period_of_activity_start': so_period_of_activities_start}]
                    source_period_of_activities_start = pd.DataFrame(source_period_of_activities_start, columns=['source_period_of_activity_start'], dtype=object)

                    source_period_of_activities_end = [{'source_period_of_activity_end': so_period_of_activities_end}]
                    source_period_of_activities_end = pd.DataFrame(source_period_of_activities_end, columns=['source_period_of_activity_end'], dtype=object)
                    
                    #from GND
                    
                    activity_period_table = pd.concat([birthdates_table['source_dateOfBirth'], deathdates_table['source_dateOfDeath'], source_period_of_activities_start, source_period_of_activities_end, birthdates_table['checked_params_birthdate'],birthdates_table['des_year_birth'], deathdates_table['checked_params_deathdate'], deathdates_table['des_year_death'] ],axis=1)
                    #activity_period_table = activity_period_table.astype(str)
                    activity_period_table['source_period_of_activity_start'] = [so_period_of_activities_start if x is np.nan else x for x in activity_period_table['source_period_of_activity_start'] ]
                    activity_period_table['source_period_of_activity_end'] = [so_period_of_activities_end if x is np.nan else x for x in activity_period_table['source_period_of_activity_end'] ]
                    
                    activity_period_table['source_period_of_activity_start']= activity_period_table['source_period_of_activity_start'].str.findall('(\d{4})')
                    activity_period_table['source_period_of_activity_end']= activity_period_table['source_period_of_activity_end'].str.findall('(\d{4})')
                    
                    activity_period_table['source_period_of_activity_start'] = activity_period_table['source_period_of_activity_start'].str.get(0)
                    activity_period_table['source_period_of_activity_end']= activity_period_table['source_period_of_activity_end'].str.get(0)

                    #-1, 0 und 1 für neue Bewertung
                    activity_period_score = []
                    score_info = pd.DataFrame()

                    activity_period_score_1 = []
                    activity_period_score_2 = []
                    activity_period_score_3 = []
                    activity_period_score_4 = []
                    activity_period_score_sum =[]

                    score_info_1 = pd.DataFrame()
                    score_info_2 = pd.DataFrame()
                    score_info_3 = pd.DataFrame()
                    score_info_4 = pd.DataFrame()

                    for row in activity_period_table.values:
                        source_dateOfBirth=int(row[0])
                        source_dateOfDeath=int(row[1])
                        source_period_of_activity_start=int(row[2])
                        source_period_of_activity_end=int(row[3])
                        checked_params_birthdate=int(row[4])
                        des_year_birth=int(row[5])
                        checked_params_deathdate=int(row[6])
                        des_year_death=int(row[7])

                        score_1=[]
                        score_2=[]
                        score_3=[]
                        score_4=[]

                        #Vorhandenes Jahr des Beginns der Wirkungszeit >= Vorhandenes Geburtsjahr 
                        #& Vorhandenes Jahr des Beginns der Wirkungszeit <=Vorhandenes Geburtsjahr +100")
                        if (source_period_of_activity_start == 9999) or (des_year_birth == 9999):
                            score_1 = 0
                        elif (source_period_of_activity_start >= des_year_birth) and (source_period_of_activity_start <= des_year_birth + 100):
                            score_1 = 1                              
                        else:
                            score_1 = -1
                        score_info=[{"period_score_1":score_1}]
                        score_info_1=pd.DataFrame(score_info)
                        activity_period_score_1.append(score_info_1)

                        #Vorhandenes Jahr des Beginns der Wirkungszeit <= Vorhandenes Sterbejahr 
                        #& Vorhandenes Jahr des Beginns der Wirkungszeit >=	Vorhandenes Sterbejahr-100")
                        if (source_period_of_activity_start == 9999) or (des_year_death == 9999):
                            score_2 = 0
                        elif (source_period_of_activity_start >= des_year_death - 100) and (source_period_of_activity_start <= des_year_death):
                            score_2 = 1                              
                        else:
                            score_2 = -1
                        score_info=[{"period_score_2":score_2}]
                        score_info_2=pd.DataFrame(score_info)
                        activity_period_score_2.append(score_info_2)

                        #Vorhandenes Jahr des Endes der Wirkungszeit <=	Vorhandenes Sterbejahr 
                        #& Vorhandenes Jahr des Endes der Wirkungszeit >= Vorhandenes Sterbejahr -100")
                        if (source_period_of_activity_end == 9999 or des_year_death == 9999):
                            score_3 = 0
                        elif (source_period_of_activity_end <= des_year_death and source_period_of_activity_end >= des_year_death - 100):
                            score_3 = 1                              
                        else:
                            score_3 = -1
                        score_info=[{"period_score_3":score_3}]
                        score_info_3=pd.DataFrame(score_info)
                        activity_period_score_3.append(score_info_3)

                        #Vorhandenes Jahr des Endes der Wirkungszeit <= Vorhandenes Geburtsjahr +100 
                        #& Vorhandenes Jahr des Endes der Wirkungszeit >= Vorhandenes Geburtsjahr")
                        if (source_period_of_activity_end == 9999 or des_year_birth == 9999):
                            score_4 = 0
                        elif (source_period_of_activity_end >= des_year_birth and source_period_of_activity_end <= des_year_birth + 100):
                            score_4 = 1                              
                        else:
                            score_4 = -1
                        score_info=[{"period_score_4":score_4}]
                        score_info_4=pd.DataFrame(score_info)
                        activity_period_score_4.append(score_info_4)

                    activity_period_score_1=pd.concat(activity_period_score_1, ignore_index=True)
                    activity_period_score_2=pd.concat(activity_period_score_2, ignore_index=True)
                    activity_period_score_3=pd.concat(activity_period_score_3, ignore_index=True)
                    activity_period_score_4=pd.concat(activity_period_score_4, ignore_index=True)
                    activity_period_score_sum=activity_period_score_1['period_score_1']+activity_period_score_2['period_score_2']+activity_period_score_3['period_score_3']+activity_period_score_4['period_score_4']
                    activity_period_score_sum=pd.DataFrame(activity_period_score_sum, columns=['sum'])
                    activity_period_score_table =pd.concat([activity_period_table['source_period_of_activity_start'], activity_period_table['source_period_of_activity_end'], activity_period_table['checked_params_birthdate'], activity_period_table['checked_params_deathdate'], birthdates_table['des_year_birth'], deathdates_table['des_year_death'], activity_period_score_1, activity_period_score_2, activity_period_score_3, activity_period_score_4, activity_period_score_sum['sum']], axis=1)
                    activity_period_score_table['activity_period_ijklmn_score'] =np.where([activity_period_score_table.sum != 0], activity_period_score_sum['sum']* 0.178, 0)

                    info_table_2 = info_table.transpose()
                    all_scores=pd.concat([names_score_tables, birthdate_score_table['G_birthdate_score'], deathdate_score_table['H_deathdate_score'],result_birthplaces['O_birthplace_score'],result_deathplaces['P_deathplace_score'],job_scores['max_job_score'], activity_period_score_table['activity_period_ijklmn_score']], axis=1)
                    all_scores['total_score']=-8.41255 + all_scores['max_name_score']*1.298 + all_scores['IstMx_x_Anz']*-0.283 + all_scores['IstMaxNameScore']*5.07+ all_scores['max_job_score']*2.778 + all_scores['G_birthdate_score']*1.139 + all_scores['H_deathdate_score']*1.2015 + all_scores['O_birthplace_score']*0.857 + all_scores['P_deathplace_score']*2.024 + all_scores['activity_period_ijklmn_score']*0.178
                    all_scores['total_score']=all_scores['total_score'].astype(float).round(decimals=2)
                    all_scores=pd.concat([all_scores, gnd_id, des_preferredName_url, des_preferredName_org, des_variant_name_org, des_birthdate_org, des_deathdate_org, des_placeOfBirth_org, des_placeOfDeath_org, des_profession_org], axis=1)
                    all_scores=all_scores.sort_values(by='total_score', ascending=False)
                    all_scores_filtered = all_scores[all_scores.total_score >= -6.0]
                    all_scores_filtered = pd.DataFrame(all_scores_filtered, columns=['gndIdentifier','id', 'preferredName', 'variantName','dateOfBirth', 'dateOfDeath','placeOfBirth', 'placeOfDeath', 'professionOrOccupation', 'total_score', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'IstMx_x_Anz', 'IstMaxNameScore', 'max_job_score','H_deathdate_score','G_birthdate_score', 'O_birthplace_score','P_deathplace_score','activity_period_ijklmn_score'])
                    all_scores_filtered =all_scores_filtered.rename(columns={"gndIdentifier": "gnd_id","id":"gnd_uri","preferredName": "preferred_name", "variantName":"variant_name", "dateOfBirth":"date_of_birth", "dateOfDeath":"date_of_death", "placeOfBirth":"place_of_birth", "placeOfDeath":"place_of_death","professionOrOccupation":"profession_or_occupation", "IstMx_x_Anz":"istmx_x_anz", "IstMaxNameScore":"istmax_name_score", "H_deathdate_score":"h_deathdate_score", "G_birthdate_score":"g_birthdate_score", "O_birthplace_score":"o_birthplace_score", "P_deathplace_score":"p_deathplace_score"}) 
                    
                    gd_df = GridOptionsBuilder.from_dataframe(all_scores_filtered)
                    gd_df.configure_pagination(enabled=True)
                    gd_df.configure_default_column(editable=True, groupable=True)
                    gd_df.configure_column("gnd_uri",
                        headerName="gnd_uri",
                        cellRenderer=JsCode("""
                    function(params) {
                        return '<a href=' + params.value + ' target="_blank">🖱️</a>'
                        }
                    """))
                    cellsytle_jscode = JsCode("""
                                            function(params) {
                                                if (params.value >= 2.5) {
                                                    return {
                                                        'color': 'black',
                                                        'backgroundColor': '#00CC00'
                                                    }
                                                }
                                                else if (params.value >= 0.5 && params.value < 2.5) {
                                                    return {
                                                        'color': 'black',
                                                        'backgroundColor': '#66FF66'
                                                    }
                                                }

                                                else if (params.value > -1.5 && params.value > 0.5) {
                                                    return {
                                                        'color': 'black',
                                                        'backgroundColor': '#CCFFCC'
                                                    }
                                                }
                                                else {
                                                    return {
                                                        'color': 'black',
                                                        'backgroundColor': 'lightpink'
                                                    }
                                                }
                                            };
                                            """)
                    gd_df.configure_column("total_score",cellStyle=cellsytle_jscode)

                    gridoptions_df = gd_df.build()

                    info_table_3=info_table_2.reset_index()
                    info_table_3=info_table_3.rename({'index':'table_index'}, axis=1)

                    meta_data_1=meta_data.reset_index()
                    meta_data_1=meta_data_1.rename({'index':'meta_index'}, axis=1)
                    all_scores_filtered_1=all_scores_filtered.reset_index()
                    target_data_source = '_lobid'

                    if save_status == 'Speichern':
                        all_scores_to_save=pd.concat([meta_data,info_table_2['dc_id'], info_table_2['person_id'],info_table_2['source_system'], all_scores_filtered_1], axis=1)
                        all_scores_to_save['dc_id']=all_scores_to_save['dc_id'].fillna(method='ffill')
                        all_scores_to_save['person_id']=all_scores_to_save['person_id'].fillna(method='ffill')
                        all_scores_to_save['date_of_query']=all_scores_to_save['date_of_query'].fillna(method='ffill')
                        all_scores_to_save['queried_by']=all_scores_to_save['queried_by'].fillna(method='ffill')
                        all_scores_to_save['default_title']=all_scores_to_save['default_title'].fillna(method='ffill')
                        all_scores_to_save['alternative_title']=all_scores_to_save['alternative_title'].fillna(method='ffill')
                        all_scores_to_save['comment_content']=all_scores_to_save['comment_content'].fillna(method='ffill')
                        all_scores_to_save['source_system']=all_scores_to_save['source_system'].fillna(method='ffill')
                        all_scores_to_save['public']=all_scores_to_save['public'].fillna(method='ffill')
                        all_scores_to_save['target_data_source'] = 'lobid'
                        match_status =" "
                        comment = " "
                        all_scores_to_save['match_status']=match_status
                        all_scores_to_save['comment']=comment
                        queried_data = pd.concat([meta_data, info_table_2], axis=1)
                        queried_data['target_data_source'] = 'lobid'
                        all_scores_to_save = all_scores_to_save.applymap(str)
                        conn=db_connect()
                        queried_data.to_sql(name='queried_private_person_data', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')
                        all_scores_to_save.to_sql(name='private_data_results', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')

                    elif save_status == 'Nicht speichern':
                        query_id_result=pd.concat([meta_data_1, query_id_result, info_table_3, all_scores_filtered_1], ignore_index=False, axis=1)
                        all_scores_table_total.append(query_id_result)
                        with st.expander(NDS_Id + " " +forename_surname_org):
                            st.info("Protokoll zum Matching")
                            key_id=uuid.uuid4()
                            key_id=str(key_id)
                            key_id_1=uuid.uuid4()
                            key_id_1=str(key_id_1)
                            st.info("Alle Wertungsergebnisse")
                            grid_table=AgGrid(all_scores_filtered, gridOptions=gridoptions_df, height=500, key= key_id_1, allow_unsafe_jscode=True)
                            
                        
                    else:
                        pass

            if save_status == 'Speichern':
                st.success('Ihre Ergebnisse wurden erfolgreich gespeichert.', icon="✅")
                def tac():
                    t_sec = round(time.time() - start_time)
                    (t_min, t_sec) = divmod(t_sec,60)
                    (t_hour,t_min) = divmod(t_min,60)
                    st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                tac()
            elif save_status == 'Nicht speichern':
                all_scores_table_total = pd.concat(all_scores_table_total)

                csv = convert_df(all_scores_table_total) 
                st.download_button("Zum Herunterladen drücken", csv, "file.csv", "text/csv")
                def tac():
                    t_sec = round(time.time() - start_time)
                    (t_min, t_sec) = divmod(t_sec,60)
                    (t_hour,t_min) = divmod(t_min,60)
                    st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                tac()