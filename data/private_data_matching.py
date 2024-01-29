import streamlit as st
import pandas as pd
import numpy as np
import uuid
import re
import string
from string import digits
import grequests
import json
from Levenshtein import distance
import jellyfish
from similar_text import similar_text
from sqlalchemy import create_engine
from itertools import combinations
from st_aggrid import AgGrid, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import ijson
import time
import datetime
import dask.dataframe as dd
import urllib.request
from math import radians, cos, sin, asin, sqrt
import folium
from streamlit_folium import st_folium
import math
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
from users.session_state import session_state
from db.db_functions import db_connect



@st.cache_data
def Convert(string):
    li = list(string.split(" "))
    return li

@st.cache_data
def Convert_str_list(string):
    li = list(string.split(","))
    return li

@st.cache_data
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km

def call_data():
    df = pd.read_sql("select * from person_private_data", con=conn)
    return df

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

def fn_private_data(data):
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
        save_status = st.radio("Ergebnisse speichern.", ('Speichern', 'Nicht speichern'), key='1')
        now = datetime.datetime.now()
        type_entity ="Person"
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

        st.info('Bitte Wählen Lobid Query Type')
        query_type = st.radio("Bitte Wählen", ('Standard', 'Limited'))

        st.info('Bitte Wählen Lobid Size Limit')
        size_limit_1= '20'
        size_limit_2= '40'
        size_limit_3= '60'
        lobid_size_limit = st.radio("Size Limit", (size_limit_1, size_limit_2, size_limit_3), horizontal=True, label_visibility="visible", key="size_type")
        
        limit_size =""
        if lobid_size_limit == size_limit_1:
            limit_size=size_limit_1
        elif lobid_size_limit == size_limit_2:
            limit_size=size_limit_2
        elif lobid_size_limit == size_limit_3:
            limit_size=size_limit_3
        elif lobid_size_limit == user_defined:
            limit_size=user_defined
       
        if st.button("Load Data"):
            st.session_state.load_state = True
            start_time = time.time()
            def tic():
                global start_time 
                start_time = time.time()

            lev_distance = "%7E2+"
            path1 = ""
            path2 = ""
            path3 = ""
            path4 = ""
            path5 = ""
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

                query_1 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_2 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_3 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+AND+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_4 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+AND+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_5 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_6 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_7 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_8 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_9 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_10 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_11 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_12 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_13 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_14 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_15 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['birthdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_16 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+OR+'+'('+'dateOfBirth:'+(df_results['birthdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'

                query_17 = 'http://lobid.org/gnd/search?q=' + 'preferredName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'
                query_18 = 'http://lobid.org/gnd/search?q=' + 'variantName:'+'('+df_results['search_terms']+')' +'+AND+'+'('+'dateOfBirth:'+(df_results['birthdate'])+'+OR+dateOfDeath:'+(df_results['deathdate'])+')'+'&filter=(type:RoyalOrMemberOfARoyalHouse+OR+LiteraryOrLegendaryCharacter+OR+DifferentiatedPerson)&size='+limit_size+'&format=json'


                df_request_1 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] == "9999*"), query_1, query_2)
                df_request_2 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] == "9999*"), query_2, query_1)
                df_request_3 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_3, query_1)
                df_request_4 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_4, query_2)
                df_request_5 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_5, query_1)
                df_request_6 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_6, query_2)
                df_request_7 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_7, query_1)
                df_request_8 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_8, query_2)
                df_request_9 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] != "9999*"), query_9, query_1)
                df_request_10 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] != "9999*"), query_10, query_2)
                df_request_11 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] != "9999*"), query_11, query_1)
                df_request_12 = np.where((df_results['birthdate'] == "9999*") & (df_results['deathdate'] != "9999*"), query_12, query_2)
                df_request_13 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] == "9999*"), query_13, query_1)
                df_request_14 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] == "9999*"), query_14, query_2) 
                df_request_15 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] == "9999*"), query_15, query_1) 
                df_request_16 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] == "9999*"), query_16, query_2) 

                df_request_17 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_17, query_1)
                df_request_18 = np.where((df_results['birthdate'] != "9999*") & (df_results['deathdate'] != "9999*"), query_18, query_2)

                df_request_1 = pd.DataFrame(df_request_1, columns=['search_terms'])
                df_request_2 = pd.DataFrame(df_request_2, columns=['search_terms'])
                df_request_3 = pd.DataFrame(df_request_3, columns=['search_terms'])
                df_request_4 = pd.DataFrame(df_request_4, columns=['search_terms'])
                df_request_5 = pd.DataFrame(df_request_5, columns=['search_terms'])
                df_request_6 = pd.DataFrame(df_request_6, columns=['search_terms'])
                df_request_7 = pd.DataFrame(df_request_7, columns=['search_terms'])
                df_request_8 = pd.DataFrame(df_request_8, columns=['search_terms'])
                df_request_9 = pd.DataFrame(df_request_9, columns=['search_terms'])
                df_request_10 = pd.DataFrame(df_request_10, columns=['search_terms'])
                df_request_11 = pd.DataFrame(df_request_11, columns=['search_terms'])
                df_request_12 = pd.DataFrame(df_request_12, columns=['search_terms'])
                df_request_13 = pd.DataFrame(df_request_13, columns=['search_terms'])
                df_request_14 = pd.DataFrame(df_request_14, columns=['search_terms'])
                df_request_15 = pd.DataFrame(df_request_15, columns=['search_terms'])
                df_request_16 = pd.DataFrame(df_request_16, columns=['search_terms'])

                df_request_17 = pd.DataFrame(df_request_17, columns=['search_terms'])
                df_request_18 = pd.DataFrame(df_request_18, columns=['search_terms'])
                
                if query_type == "Standard":
                    df_all_requests=pd.concat([df_request_1, df_request_2, df_request_3, df_request_4, df_request_5, df_request_6, df_request_7, df_request_8, df_request_9, df_request_10, df_request_11, df_request_12, df_request_13, df_request_14, df_request_15, df_request_16], axis=0)

                elif query_type == "Limited":
                    df_all_requests=pd.concat([df_request_1, df_request_2, df_request_17, df_request_18, df_request_9, df_request_10, df_request_13, df_request_14], axis=0)
                df_all_requests=pd.DataFrame(df_all_requests, columns=['search_terms'])
                df_all_requests = df_all_requests.drop_duplicates(subset=['search_terms'],keep='first')
                
                time.sleep(1)
                num_sessions=150
                requests_ = (grequests.get(url,timeout=60) for url in df_all_requests['search_terms'].values)                  
                responses = grequests.imap(requests_, size=num_sessions*5)
                if responses is not None:          
                    gnd_data = [response.json() for response in responses]
                else:
                    pass

                if gnd_data is not None:
                    query_id_result= pd.json_normalize(gnd_data)
                else:
                    pass
                query_id_result = pd.DataFrame(query_id_result, columns=['id', 'totalItems']).astype(str)
                query_id_result['total_items']=query_id_result['totalItems'].astype(int)
                query_id_result['url'] = query_id_result['id'].str.replace("&format=json", "")
                query_id_result=query_id_result.reset_index()
                query_id_result=query_id_result.rename({'id':'query_id'}, axis=1)
                query_id_result=query_id_result.rename({'index':'query_index'}, axis=1)
                gd = GridOptionsBuilder.from_dataframe(query_id_result)
                gd.configure_pagination(enabled=True)
                gd.configure_default_column(editable=True, groupable=True)
                gd.configure_column("url",
                                    headerName="query_url",
                                    cellRenderer=JsCode("""
                                function(params) {
                                    return '<a href=' + params.value + ' target="_blank">🖱️</a>'
                                    }
                                """))
                gd.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True)
                gridoptions = gd.build()

                target_data= pd.json_normalize(gnd_data, record_path =['member'])
                df_nested_list_gnd = pd.DataFrame(target_data)
                df = df_nested_list_gnd.drop_duplicates(subset=['gndIdentifier'],keep='first')
                df=df.reset_index()

                if 'placeOfDeath' not in df:
                    df['placeOfDeath'] =pd.Series([ [{'id': 'No id', 'label': 'No Deathplace'}] ])

                if 'placeOfBirth' not in df:
                    df['placeOfBirth'] = pd.Series([ [{'id': 'No id', 'label': 'No Birthplace'}] ])

                if 'dateOfBirth' not in df:
                    df['dateOfBirth'] =np.nan

                if 'dateOfDeath' not in df:
                    df['dateOfDeath'] =np.nan

                if 'professionOrOccupation' not in df:
                    df['professionOrOccupation'] =pd.Series([ [{'id': 'No id', 'label': 'No Profession'}] ])

                if 'gndIdentifier' in df and 'preferredName' in df and 'variantName' in df and 'dateOfBirth' in df and 'placeOfBirth' in df and 'dateOfDeath' in df and 'placeOfDeath' in df and 'professionOrOccupation' in df:             
                    lev_threshold = 2
                    metaphone_threshold = 90
                    year_threshold = 5
                    check_params_name=2
                    check_params_profession = 1

                    #from nds
                    #preferred names
                    so_preferredName = [{'source_preferredName': surname_name}]
                    so_preferredName=pd.DataFrame(so_preferredName, columns=['source_preferredName'])
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].astype(str)
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].map(lambda x: x.lower())
                    so_preferredName['source_preferredName']=so_preferredName['source_preferredName'].map(lambda x: x.strip())

                    #non-preferred names
                    so_nonpreferredName = [{'source_nonpreferredName': source_non_preferred_names}]
                    so_nonpreferredName=pd.DataFrame(so_nonpreferredName, columns=['source_nonpreferredName'])
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].astype(str)
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].map(lambda x: x.lower())
                    so_nonpreferredName['source_nonpreferredName']=so_nonpreferredName['source_nonpreferredName'].map(lambda x: x.strip())
                    
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
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].map(lambda x: x.lower())
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].map(lambda x: x.strip())
                    preferredName_table['source_preferredName'] = preferredName_table['source_preferredName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                    preferredName_table.reset_index(inplace=True, drop=True)

                    preferredName_table['check_params_preferred_name'] = np.where((preferredName_table['source_preferredName'] != type(None)) & (preferredName_table['dest_preferredName'] != type(None)), check_params_name, 0)

                    non_preferredName_table = pd.concat([so_nonpreferredName, des_preferredName],axis=1)
                    non_preferredName_table['source_nonpreferredName'] = non_preferredName_table['source_nonpreferredName'].fillna(value=source_non_preferred_names)

                    so_preferredName =so_preferredName.source_preferredName.str.split(expand=True)
                    so_preferredName = so_preferredName.values.tolist()

                    so_nonpreferredName=so_nonpreferredName.source_nonpreferredName.str.split(expand=True)
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
                    score_table_pref_names=pd.concat([preferredName_table,pref_name_score_table],axis=1)

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

                    des_variant_name = pd.DataFrame(df, columns=['variantName'])
                    des_variant_name_org = pd.DataFrame(df, columns=['variantName'])

                    so_preferredName = [{'source_preferredName': surname_name}]
                    so_preferredName=pd.DataFrame(so_preferredName, columns=['source_preferredName'])
                    des_variant_name_explode = pd.DataFrame(df, columns=['variantName'])
                    des_variant_name_explode = des_variant_name_explode.explode('variantName')
                    variantName_table = pd.concat([so_preferredName, des_variant_name_explode], axis=1)
                    variantName_table = variantName_table.fillna(value=surname_name)
                    #variantName_table = variantName_table.apply(lambda x: x.replace({' van ':'', ' el ':'', 'von':'', ' al ':'', ' ibn ':'', ' abd ':'', ' und ':' '}, regex=True))
                    variantName_table['source_preferredName'] = variantName_table['source_preferredName'].map(lambda x: x.lower())
                    variantName_table['source_preferredName'] = variantName_table['source_preferredName'].map(lambda x: x.strip())
                    variantName_table['source_preferredName'] = variantName_table['source_preferredName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)

                    so_preferredName_df = [{'source_preferredName': surname_name}]
                    so_preferredName_df=pd.DataFrame(so_preferredName_df, columns=['source_preferredName'])
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].astype(str)
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].map(lambda x: x.lower())
                    so_preferredName_df['source_preferredName']=so_preferredName_df['source_preferredName'].map(lambda x: x.strip()) 
                    so_preferredName_df =so_preferredName_df.source_preferredName.str.split(expand=True)
                    
                    des_variant_name_explode['variantName'] = des_variant_name_explode['variantName'].replace(np.nan, 'VariantName, NotDescribed')
                    des_variant_name_explode['variantName'] = des_variant_name_explode['variantName'].astype(str)
                    des_variant_name_explode['des_variantName'] = [" ".join(n.split(", ")[::-1]) for n in des_variant_name_explode['variantName']]
                    des_variant_name_explode['des_variantName'] = des_variant_name_explode['des_variantName'].map(lambda x: x.lower())
                    des_variant_name_explode['des_variantName'] = des_variant_name_explode['des_variantName'].map(lambda x: x.strip())
                    des_variant_name_explode['des_variantName'] = des_variant_name_explode['des_variantName'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                    des_variant_name_explode_df=des_variant_name_explode['des_variantName']
                    des_variant_name_explode=des_variant_name_explode['des_variantName'].str.split(expand=True)
                    des_variant_name_explode_df=des_variant_name_explode.reset_index()
                    so_preferredName_df = so_preferredName_df.values.tolist()
                    des_variant_name_explode=des_variant_name_explode.values.tolist()

                    var_name_score=[]
                    var_name_scores=[]
                    for items in des_variant_name_explode:
                        filtered_list = list(filter(None, items))
                        list_1 = so_preferredName_df[0]
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
                    var_name_score_table = pd.concat(var_name_scores, ignore_index=True)
                    var_name_score_table =pd.DataFrame(var_name_score_table,index=des_variant_name_explode_df.index.copy())
                    score_table_pref_variant_names=pd.concat([so_preferredName,des_variant_name_explode_df,var_name_score_table],axis=1, ignore_index=False)
                    score_table_pref_variant_names['source_preferredName']=score_table_pref_variant_names['source_preferredName'].fillna(method='ffill')


                    score_table_pref_variant_names=score_table_pref_variant_names.reset_index()
                    score_table_pref_variant_names_max = score_table_pref_variant_names.groupby(['index'])['var_name_score'].max()
                    score_table_pref_variant_names_max =pd.DataFrame(score_table_pref_variant_names_max, columns=["var_name_score"])

                    #this is for non-pref-name versus var-name
                    non_pref_var_name_score=[]
                    non_pref_var_name_scores=[]

                    for items in des_variant_name_explode:
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
                    non_pref_var_name_score_table =pd.DataFrame(non_pref_var_name_score_table,index=des_variant_name_explode_df.index.copy())
                                        
                    score_table_nonpref_variant_names=pd.concat([des_variant_name_explode_df,non_pref_var_name_score_table],axis=1, ignore_index=False)
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
                    so_dateOfBirth = [{'source_dateOfBirth': birthdate}]
                    so_dateOfBirth=pd.DataFrame(so_dateOfBirth, columns=['source_dateOfBirth'])

                    #from GND

                    des_birthdate = df['dateOfBirth']
                    des_birthdate_org = df['dateOfBirth']
                    des_birthdate = pd.DataFrame(des_birthdate, columns=['dateOfBirth'])
                    

                    birthdate_data = []
                    for row in des_birthdate.values:
                        des_dateOfBirth = str(row[0])
                        if des_dateOfBirth == 'nan':
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
                    birthdates_table['delta'] = abs(birthdates_table['delta'])

                    birthdates_table['source_dateOfBirth'] = birthdates_table['source_dateOfBirth'].astype('Int64').astype(str)
                    birthdates_table['des_year_birth'] = birthdates_table['des_year_birth'].astype('Int64').astype(str)

                    birthdates_table['Difference'] = np.where(birthdates_table['source_dateOfBirth'] == birthdates_table['des_year_birth'] , 1, 0)
                    birthdates_table['checked_params_birthdate_source'] = np.where((birthdates_table['source_dateOfBirth'] == "9999"), 0, 1)
                    birthdates_table['checked_params_birthdate_des'] = np.where((birthdates_table['des_year_birth'] == "9999"), 0, 1)
                    birthdates_table['checked_params_birthdate'] = np.where(((birthdates_table.checked_params_birthdate_source > 0) & (birthdates_table.checked_params_birthdate_des > 0)), 1, 0)
                    birthdate_score = []
                    for row in birthdates_table.values:
                        source_dateOfBirth=int(row[0])
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
                    so_dateOfDeath = [{'source_dateOfDeath': deathdate}]
                    so_dateOfDeath=pd.DataFrame(so_dateOfDeath, columns=['source_dateOfDeath'])

                    #from GND
                    des_deathdate = df['dateOfDeath']
                    des_deathdate_org = df['dateOfDeath'] 
                    des_deathdate = pd.DataFrame(des_deathdate, columns=['dateOfDeath'])

                    deathdate_data = []
                    for row in des_deathdate.values:
                        des_dateOfDeath = str(row[0])
                        if des_dateOfDeath == 'nan':
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
                    deathdates_table['delta'] = abs(deathdates_table['delta'])
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
                    #from NDS
                    so_placeOfBirth = [{'source_placeOfBirth': birthplace}]
                    nds_id=[{'dc_id': dc_id}]
                    so_placeOfBirth=pd.DataFrame(so_placeOfBirth, columns=['source_placeOfBirth'])
                    nds_id=pd.DataFrame(nds_id, columns=['dc_id'])
                    so_placeOfBirth=pd.concat([nds_id, so_placeOfBirth], axis=1)
                    so_placeOfBirth=pd.DataFrame(so_placeOfBirth, columns=['source_placeOfBirth']) 

                    #from GND
                    des_placeOfBirth = df['placeOfBirth']
                    des_placeOfBirth_org = df['placeOfBirth']
                    des_placeOfBirth = pd.DataFrame(des_placeOfBirth, columns=['placeOfBirth'])
                    des_placeOfBirth['placeOfBirth'] = [ [{'id': 'No id', 'label': 'No Birthplace'}] if x is np.nan else x for x in des_placeOfBirth['placeOfBirth'] ]
                    y=des_placeOfBirth['placeOfBirth'].iloc[:]
                    des_placeOfBirth_data = []
                    for data in y:
                        des_placeOfBirth_data += [pd.DataFrame(data)]     
                    des_placeOfBirth_data = pd.concat(des_placeOfBirth_data)
                    des_placeOfBirth_data = des_placeOfBirth_data.drop(des_placeOfBirth_data.index[np.where(des_placeOfBirth_data.index > 0)[0]])
                    des_placeOfBirth_data.reset_index(inplace=True, drop=True)
                    des_placeOfBirth_data.set_axis(["id","label"], axis=1,copy=True)
                    des_placeOfBirth_data['des_placeOfBirth'] = des_placeOfBirth_data['label']
                    birthplaces_table = pd.concat([so_placeOfBirth['source_placeOfBirth'], des_placeOfBirth_data['des_placeOfBirth']],axis=1)
                    birthplaces_table =pd.DataFrame(birthplaces_table, columns=['source_placeOfBirth','des_placeOfBirth'])
                    birthplaces_table['source_placeOfBirth']=birthplaces_table['source_placeOfBirth'].fillna(method='ffill')

                    birthplace_score = []
                    for row in birthplaces_table.values:
                        source_placeOfBirth=str(row[0])
                        des_placeOfBirth=str(row[1])
                        score=[]
                        if source_placeOfBirth == 'Not Described' or des_placeOfBirth == 'No Birthplace':
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
                    result_birthplaces = pd.concat([birthplaces_table, O_score], axis=1) 

                    #this is for deathplaces
                    #from NDS
                    so_placeOfDeath = [{'source_placeOfDeath': deathplace}]
                    so_placeOfDeath=pd.DataFrame(so_placeOfDeath, columns=['source_placeOfDeath'])
                    nds_id=[{'dc_id': dc_id}]
                    nds_id=pd.DataFrame(nds_id, columns=['dc_id'])
                    so_placeOfDeath=pd.concat([nds_id, so_placeOfDeath], axis=1)
                    so_placeOfDeath=pd.DataFrame(so_placeOfDeath, columns=['source_placeOfDeath']) 

                    #from GND
                    des_placeOfDeath = df['placeOfDeath']
                    des_placeOfDeath_org = df['placeOfDeath']
                    des_placeOfDeath = pd.DataFrame(des_placeOfDeath, columns=['placeOfDeath'])

                    des_placeOfDeath.loc[des_placeOfDeath['placeOfDeath'].isnull(),'value_is_NaN'] = 'Yes'
                    des_placeOfDeath.loc[des_placeOfDeath['placeOfDeath'].notnull(), 'value_is_NaN'] = 'No'
                    des_placeOfDeath['placeOfDeath'] = np.where((des_placeOfDeath['value_is_NaN'] == "Yes"), None, des_placeOfDeath['placeOfDeath'])

                    des_placeOfDeath['placeOfDeath'] = [ [{'id': 'No id', 'label': 'No Deathplace'}] if x is np.nan else x for x in des_placeOfDeath['placeOfDeath'] ]
                    des_placeOfDeath['placeOfDeath'] = [ [{'id': 'No id', 'label': 'No Deathplace'}] if x is None else x for x in des_placeOfDeath['placeOfDeath'] ]
                    y=des_placeOfDeath['placeOfDeath'].iloc[0:]
                    des_placeOfDeath_data = []
                    for data in y:
                        des_placeOfDeath_data += [pd.DataFrame(data)]     
                    des_placeOfDeath_data = pd.concat(des_placeOfDeath_data)
                    des_placeOfDeath_data = des_placeOfDeath_data.drop(des_placeOfDeath_data.index[np.where(des_placeOfDeath_data.index > 0)[0]])
                    des_placeOfDeath_data.reset_index(inplace=True, drop=True)
                    des_placeOfDeath_data.set_axis(["id","label"], axis=1,copy=True)
                    des_placeOfDeath_data['des_placeOfDeath'] = des_placeOfDeath_data['label']
                    deathplaces_table = pd.concat([so_placeOfDeath['source_placeOfDeath'], des_placeOfDeath_data['des_placeOfDeath']],axis=1) 
                    deathplaces_table= pd.DataFrame(deathplaces_table, columns=['source_placeOfDeath','des_placeOfDeath'])
                    deathplaces_table['source_placeOfDeath']=deathplaces_table['source_placeOfDeath'].fillna(method='ffill')

                    deathplace_score = []
                    for row in deathplaces_table.values:
                        source_placeOfDeath=str(row[0])
                        des_placeOfDeath=str(row[1])
                        score=[]
                        if source_placeOfDeath == 'Not Described' or des_placeOfDeath == 'No Deathplace':
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
                    result_deathplaces = pd.concat([deathplaces_table, P_score], axis=1)    

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

                    so_profession_nds_id = [{'nds_id': dc_id}]
                    so_profession_nds_id =pd.DataFrame(so_profession_nds_id, columns=['nds_id'])
                    so_profession = pd.concat([so_profession_org, so_profession_nds_id, job_list_match], axis=1)
                    so_profession = pd.DataFrame(so_profession, columns=['nds_id','so_profession', 'org_prof_name','org_prof_code'])
                    so_profession['nds_id']=so_profession['nds_id'].fillna(method='ffill')
                    so_profession.reset_index(inplace=True, drop=True)
                    so_profession1 =so_profession
                                
                    #from GND
                    des_profession = df['professionOrOccupation']
                    des_profession_org = df['professionOrOccupation']
                    gnd_id = df['gndIdentifier']
                    des_profession = pd.DataFrame(des_profession, columns=['professionOrOccupation'])
                    des_profession['professionOrOccupation'] = [ [{'id': 'No id', 'label': 'No Profession'}] if x is np.nan else x for x in des_profession['professionOrOccupation'] ]
                    professions_exploded=des_profession.explode('professionOrOccupation').iloc[:]
                    professions_exploded=pd.DataFrame(professions_exploded, columns=['professionOrOccupation']).astype(str)
                    professions_exploded['professionOrOccupation']=professions_exploded['professionOrOccupation'].iloc[0:].astype(str).apply(lambda st: st[st.find("'label': '"):st.find("'}")])
                    professions_exploded=professions_exploded['professionOrOccupation'].str.replace("'label': '", '')
                    professions_exploded_df=professions_exploded.reset_index()
                    
                    new_list2=[]

                    for element in professions_exploded.values:
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
                        all_scores_to_save.to_sql(name='private_person_data_results', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')

                    elif save_status == 'Nicht speichern':
                        query_id_result=pd.concat([meta_data_1, query_id_result, info_table_3, all_scores_filtered_1], ignore_index=False, axis=1)
                        all_scores_table_total.append(query_id_result)
                        with st.expander(NDS_Id + " " +forename_surname_org):
                            st.info("Protokoll zum Matching")
                            key_id=uuid.uuid4()
                            key_id=str(key_id) 
                            key_id_1=uuid.uuid4()
                            key_id_1=str(key_id_1)
                            grid_table=AgGrid(query_id_result, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True, key=key_id)
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

def fun_wikidata_matching_scoring(data):
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
        save_status = st.radio("Ergebnisse speichern.", ('Speichern', 'Nicht speichern'), key='1')
        now = datetime.datetime.now()
        type_entity ="Person"
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
            query_candidates =query_candidates.drop_duplicates(subset=['nds_id'],keep='first')

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
            options =query_candidates['nds_id'].unique()
            selected_options =st.multiselect("Objekt Id Wählen", options)
            query_candidates = query_candidates.loc[query_candidates["nds_id"].isin(selected_options)]
        else:
            pass

        if save_status == 'Speichern' or save_status == 'Nicht speichern':
            load = st.button("Load Data")
            if load:
                st.info("Bitte warten Sie, bis der Abfrage-, Zuordnungs- und Bewertungsprozess beendet ist, und verlassen Sie die Seite nicht.")
                st.session_state.load_state = True
                start_time = time.time()
                def tic():
                    global start_time 
                    start_time = time.time()
                all_scores = pd.DataFrame()
                lev_distance = "%7E2+"                    
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

                    mylist = [surname_name, birthdate, deathdate]
                    mystring = '"' + '" "'.join(mylist) + '"'
                    endpoint_url = "https://query.wikidata.org/sparql"
                        
                    query = """SELECT ?item ?label ?dob ?dod ?pob ?pobLabel ?pod ?podLabel
                        WHERE {
                        VALUES ?term { %s }
                        SERVICE wikibase:mwapi {
                            bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                            wikibase:api "EntitySearch";
                                            mwapi:search ?term;
                                            mwapi:language "de".
                            ?item wikibase:apiOutputItem mwapi:item.
                            ?num wikibase:apiOrdinal true.
                            ?label wikibase:apiOutput mwapi:label.
                        }
                        ?item wdt:P31 wd:Q5.
                        OPTIONAL {?item wdt:P569 ?dob}.
                        OPTIONAL {?item wdt:P570 ?dod}.
                        OPTIONAL {?item wdt:P19 ?pob}.
                        OPTIONAL {?item wdt:P20 ?pod}.
                        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }
                        } ORDER BY ASC(?num) LIMIT 50""" % mystring

                    def get_results(endpoint_url, query):
                        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
                        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
                        sparql.setQuery(query)
                        sparql.setReturnFormat(JSON)
                        return sparql.query().convert()

                    results = get_results(endpoint_url, query)
                    results_df = pd.json_normalize(results['results']['bindings'])
                    results_df = results_df.drop_duplicates(subset=['item.value'],keep='first')
                    results_df.reset_index()

                    info_table=info_table.transpose()
                    queried_data = pd.concat([meta_data, info_table], axis=1)
                    queried_data['target_data_source'] = 'wikidata'

                    conn = db_connect()
                    queried_data.to_sql(name='queried_private_person_data', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')
                    df_to_save=pd.DataFrame(results_df, columns=['label.value', 'item.value','dob.value','dod.value','pob.value','pod.value','pobLabel.value','podLabel.value'])
                    df_to_save=pd.DataFrame(df_to_save).rename(columns={"label.value":"label_value", "item.value":"item_value","dob.value":"dob_value","dod.value":"dod_value","pob.value":"pob_value", "pod.value":"pod_value", "pobLabel.value":"pobLabel_value", "podLabel.value":"podLabel_value"})

                    if 'label.value' in results_df and save_status == 'Speichern':                        
                        df_to_save=pd.concat([meta_data,info_table['dc_id'], info_table['person_id'],info_table['source_system'], df_to_save], axis=1)
                        df_to_save['dc_id']=df_to_save['dc_id'].fillna(method='ffill')
                        df_to_save['person_id']=df_to_save['person_id'].fillna(method='ffill')
                        df_to_save['date_of_query']=df_to_save['date_of_query'].fillna(method='ffill')
                        df_to_save['queried_by']=df_to_save['queried_by'].fillna(method='ffill')
                        df_to_save['default_title']=df_to_save['default_title'].fillna(method='ffill')
                        df_to_save['alternative_title']=df_to_save['alternative_title'].fillna(method='ffill')
                        df_to_save['comment_content']=df_to_save['comment_content'].fillna(method='ffill')
                        df_to_save['source_system']=df_to_save['source_system'].fillna(method='ffill')
                        df_to_save['public']=df_to_save['public'].fillna(method='ffill')
                        df_to_save['target_data_source'] = 'wikidata'
                        df_to_save.to_sql(name='private_person_data_results_wikidata', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')


                    elif save_status == 'Nicht speichern':
                        with st.expander(NDS_Id + " " + forename_surname_org):
                            st.write(df_to_save)
                
                if save_status == 'Speichern':
                    def fn_bar():
                        my_bar = st.progress(0)
                        for percent_complete in range(100):
                            time.sleep(0.1)
                            my_bar.progress(percent_complete + 1)
                    fn_bar()
                    st.success('Ihre Ergebnisse wurden erfolgreich gespeichert.', icon="✅")
                    def tac():
                        t_sec = round(time.time() - start_time)
                        (t_min, t_sec) = divmod(t_sec,60)
                        (t_hour,t_min) = divmod(t_min,60)
                        st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                    tac()

                elif save_status == 'Nicht speichern':
                    def tac():
                        t_sec = round(time.time() - start_time)
                        (t_min, t_sec) = divmod(t_sec,60)
                        (t_hour,t_min) = divmod(t_min,60)
                        st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                    tac()

def dd2dms(longitude, latitude):
    split_degx = math.modf(longitude)
    degrees_x = int(split_degx[1])
    if len(str(degrees_x)) <= 2:
        degrees_x_1 = '0'+str(degrees_x)
    else:
        degrees_x_1 = degrees_x
    minutes_x = abs(int(math.modf(split_degx[0] * 60)[1]))

    if len(str(minutes_x)) == 1:
        minutes_x = '0'+str(minutes_x)
    else:
        minutes_x =minutes_x
    seconds_x = abs(round(math.modf(split_degx[0] * 60)[0] * 60))

    split_degy = math.modf(latitude)
    degrees_y = int(split_degy[1])
    if len(str(degrees_x)) <= 2:
        degrees_y_1 = '0'+str(degrees_y)
    else:
        degrees_y_1 = degrees_y
    minutes_y = abs(int(math.modf(split_degy[0] * 60)[1]))
    if len(str(minutes_y)) == 1:
        minutes_y = '0'+str(minutes_y)
    else:
        minutes_y =minutes_y
    seconds_y = abs(round(math.modf(split_degy[0] * 60)[0] * 60))

    # account for E/W & N/S
    if degrees_x < 0:
        EorW = "W"
    else:
        EorW = "E"

    if degrees_y < 0:
        NorS = "S"
    else:
        NorS = "N"

    # abs() remove negative from degrees, was only needed for if-else above
    x= str(EorW + " " + str(degrees_x_1) + u"\u00b0 " + str(minutes_x) + "' " + str(abs(seconds_x))+ "\" ")
    y= str(NorS + " " + str(degrees_y_1) + u"\u00b0 " + str(minutes_y) + "' " + str(abs(seconds_y))+ "\" ")
    return x, y

def fn_bar():
    my_bar = st.progress(0)
    for percent_complete in range(100):
        time.sleep(0.1)
        my_bar.progress(percent_complete + 1)

def fn_buildings_private_data_lobid(data):
    username=session_state.username
    geofabrik_url= st.text_input('Please enter URL', key='geofabrik_url')
    if geofabrik_url:
        df = data
        grouped = df.groupby(['alternative_title'])
        group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
        objects = st.container()
        object_list = df['alternative_title'].unique()
        object_list =pd.DataFrame(object_list)
        free_df="<Wählen>"
        free_df=[free_df]
        free_df = pd.DataFrame(free_df)
        object_list=pd.concat([free_df,object_list],axis=0)
        
        choice = objects.selectbox("Choose a Dataset", object_list)
        if choice=="<Wählen>":
            st.write("Bitte wählen Sie einen zu verarbeitenden Datensatz aus.")
        else:
            st.write("ausgewählte private Daten:", choice)
            rows_count = df.count()[0]
            rows_count == df[df.columns[0]].count()

            st.warning("Bitte wählen Sie, ob Sie die Ergebnisse speichern möchten.")
            save_status = st.radio("Ergebnisse speichern.", ('Speichern', 'Nicht speichern'), key='2')
            now = datetime.datetime.now()
            type_entity ="Bauwerke"
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

            limit_type = st.radio("Limit", ('Standard', 'Random'), key='buildings')
            df = grouped.get_group(choice)
            df['index'] =df['dc_identifier'].astype(str)
            df =df.sort_values(by='index', ascending=True)
            df =df.drop_duplicates(subset=['dc_identifier'],keep='first')
            df_indexed = df.reset_index(drop=True, inplace=True)
            rows_count = df.count()[0]
            rows_count == df[df.columns[0]].count()
            
            st.write("Die Gesamtanzahl der Zeilen in diesem Datensatz beträgt:", rows_count)
            limit_slider_standard=""
            limit_slider_random =""
            if limit_type == 'Standard':
                limit = limit_type
                slider_range=st.slider("double ended slider", value=[0, (int(rows_count))])
                st.write("slider range:", slider_range, slider_range[0], slider_range[1])
                slider_range_1=slider_range[0]
                slider_range_2=slider_range[1]
                candidates=df.iloc[slider_range_1:slider_range_2]
                selected_rows_count = candidates[candidates.columns[0]].count()
                limit_slider_standard =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))
            
            elif limit_type == 'Random':
                limit = limit_type
                range_start = st.number_input('Range Start', value=0, key='random_1')
                range_end = st.number_input('Range End', value=(int(rows_count)), key='random_2')
                slider_range=st.slider("double ended slider", value=[range_start, range_end])
                st.write("slider range:", slider_range, slider_range[0], slider_range[1])
                slider_range_1=slider_range[0]
                slider_range_2=slider_range[1]
                candidates=df.iloc[slider_range_1:slider_range_2]
                selected_rows_count = candidates[candidates.columns[0]].count()
                limit_slider_random =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))

            if limit_slider_standard:
                query_candidates=candidates.iloc[0:limit_slider_standard]
                st.write(query_candidates)
            elif limit_slider_random:
                query_candidates = candidates.sample(n=limit_slider_random, replace=True, random_state=1)
                query_candidates = st.data_editor(query_candidates)
                query_candidates =query_candidates.astype(str)
                query_candidates =query_candidates.drop_duplicates(subset=['dc_identifier'],keep='first')

            selected = st.radio("Bitte Wählen", ('nach alternate Name filtern', 'nach alternate Name filtern (by search)', 'nach alternate Platz filtern', 'nach Objekt Id filtern', 'nach Date of Production filtern'))
            if selected == 'nach alternate Name filtern':
                query_candidates =query_candidates.astype(str)       

            elif selected == 'nach alternate Name filtern (by search)':
                query_candidates =query_candidates.astype(str)
                options =query_candidates['alternate_name'].unique()
                selected_options =st.multiselect("Name Wählen", options)
                query_candidates = query_candidates.loc[query_candidates["alternate_name"].isin(selected_options)]

            elif selected == 'nach alternate Platz filtern':
                query_candidates =query_candidates.astype(str)
                options =query_candidates['abbreviated_name'].unique()
                selected_options =st.multiselect("Name Wählen", options)
                query_candidates = query_candidates.loc[query_candidates['abbreviated_name'].isin(selected_options)]

            elif selected == 'nach Objekt Id filtern':                    
                query_candidates =query_candidates.astype(str)
                options =query_candidates['dc_identifier'].unique()
                selected_options =st.multiselect("Objekt Id Wählen", options)
                query_candidates = query_candidates.loc[query_candidates["dc_identifier"].isin(selected_options)]
            elif selected == 'nach Date of Production filtern':
                st.write("Thisfilter is under construction.")
            else:
                pass
                        
            if st.button("Load Data", key='load_1'):
                st.info("Bitte warten Sie, bis der Abfrage-, Zuordnungs- und Bewertungsprozess beendet ist, und verlassen Sie die Seite nicht.")
                start_time = time.time()
                def tic():
                    global start_time 
                    start_time = time.time()
                for row in query_candidates.values:
                    chars = {'ö':'oe','ä':'ae','ü':'ue','Ü':'ue','ß':'ss', 'é':'e','-':'',' der ':' ',' St ':' ',' an ':' ','Ev':' ', ' und ':' '}
                    id=row[1]
                    gnd_id=row[2]
                    gnd_id_org=str(gnd_id)
                    name=""
                    place=""
                    name=row[3]
                    name_org=row[3]
                    name_1=name
                    place=row[4]               
                    place_org=row[4]
                    place_1=place
                    
                    if name == 'None' and place == 'None':
                        name = row[7]
                        name_org = row[7]
                        name_1=name
                        
                        place =""
                        place_1=place
                        place_org=place
                        
                        name = name.replace('&', '')
                        pattern = r'[' + string.punctuation + ']'
                        name = re.sub(pattern, ' ', name)
                        name= ''.join(i for i in name if not i.isdigit())
                        name = name.strip()
                        names=Convert(name)
                        name=[name for name in names if len (name) > 1]
                        name=" ".join(name)

                        name_org= name_org.replace('&', '')
                        name_org = re.sub(pattern, ' ', name_org)
                        name_org= ''.join(i for i in name_org if not i.isdigit())
                        name_org=name_org.strip()
                        names_org=Convert(name_org)
                        name_org=[name_org for name_org in names_org if len (name_org) > 1]
                        name_org=" ".join(name_org)

                        place = place.replace('&', '')
                        place = re.sub(pattern, ' ', place)
                        place= ''.join(i for i in place if not i.isdigit())
                        place = place.strip()

                        name_place = str(name)
                        name_place =name_place.replace('é','e')
                        name_place=" ".join(name_place.split())
                        name_place= str(name_place).replace(' ','%20')
                        name_place_1= str(name_place).replace(' ','%20')
                    elif name == 'None' and place != 'None':
                        name = row[7]
                        name_org = row[7]
                        name_1=name
                        
                        place =place
                        place_1=place
                        place_org=place
                        
                        name = name.replace('&', '')
                        pattern = r'[' + string.punctuation + ']'
                        name = re.sub(pattern, ' ', name)
                        name= ''.join(i for i in name if not i.isdigit())
                        name = name.strip()
                        names=Convert(name)
                        name=[name for name in names if len (name) > 1]
                        name=" ".join(name)

                        name_org= name_org.replace('&', '')
                        name_org = re.sub(pattern, ' ', name_org)
                        name_org= ''.join(i for i in name_org if not i.isdigit())
                        name_org=name_org.strip()
                        names_org=Convert(name_org)
                        name_org=[name_org for name_org in names_org if len (name_org) > 1]
                        name_org=" ".join(name_org)

                        place = place.replace('&', '')
                        place = place.replace('-', '')
                        place = re.sub(pattern, ' ', place)
                        place= ''.join(i for i in place if not i.isdigit())
                        place = place.strip()

                        name_place = str(name)
                        name_place=" ".join(name_place.split())
                        name_place= str(name_place).replace(' ','%20')
                        name_place_1= str(name_place).replace(' ','%20')
                    elif name != 'None' and place == 'None':
                        name = name
                        name_org = name
                        name_1=name
                        
                        place =row[7]
                        place_1=place
                        place_org=place
                        
                        name = name.replace('&', '')
                        pattern = r'[' + string.punctuation + ']'
                        name = re.sub(pattern, ' ', name)
                        name= ''.join(i for i in name if not i.isdigit())
                        name = name.strip()
                        names=Convert(name)
                        name=[name for name in names if len (name) > 1]
                        name=" ".join(name)

                        name_org= name_org.replace('&', '')
                        name_org = re.sub(pattern, ' ', name_org)
                        name_org= ''.join(i for i in name_org if not i.isdigit())
                        name_org=name_org.strip()
                        names_org=Convert(name_org)
                        name_org=[name_org for name_org in names_org if len (name_org) > 1]
                        name_org=" ".join(name_org)

                        place = place.replace('&', '')
                        place = place.replace('-', '')
                        place = re.sub(pattern, ' ', place)
                        place= ''.join(i for i in place if not i.isdigit())
                        place = place.strip()

                        name_place = str(name)
                        name_place=" ".join(name_place.split())
                        name_place= str(name_place).replace(' ','%20')
                        name_place_1= str(name_place).replace(' ','%20')
                    else:
                        name = name
                        name_org = name
                        name_1=name
                        place =place
                        place_1=place
                        place_org=place
                        name_place = str(name)+'%20'+str(place)
                        name_place= str(name_place).replace(' ','%20')
                        name_place_1= str(name_place).replace(' ','%20')

                    street_address=row[18]
                    street_address = street_address.strip()
                    street_address_org=row[18]

                    for char in chars:
                        name_place = name_place.replace(char,chars[char])
                    name=str(name)
                    for char in chars:
                        name = name.replace(char,chars[char])

                    place=str(place)
                    for char in chars:
                        place = place.replace(char,chars[char])

                    street_address=str(street_address)
                    for char in chars:
                        street_address = street_address.replace(char,chars[char])
                    name_place_1 = str(name_place_1)
                    for char in chars:
                        name_place_1 = name_place_1.replace(char,chars[char])

                    name= str(name).replace('/','')
                    name=" ".join(name.split())

                    name= str(name).replace(' ','%20')
                    place= str(place).replace('/','')
                    place=" ".join(place.split())
                    place= str(place).replace(' ','%20')
                    street_address= str(street_address).replace('/','')
                    street_address= str(street_address).replace('  ','')
                    separator = '&'
                    street_address = street_address.split(separator, 1)[0]
                    street_address=" ".join(street_address.split())
                    street_address= str(street_address).replace(' ','%20')

                    lat=row[22]
                    lat_org=row[22]
                    lon=row[23]
                    lon_org=row[23]
                    var_geo_name_org=row[9]
                    geo_name_literal_org =row[16]
                    geo_name_literal_org= str(geo_name_literal_org).replace('Lkr.','')
                    geo_name_literal_org= str(geo_name_literal_org).replace('.','')
                    address_name_search=""
                    
                    result=[]

                    info_table = [{'id':id, 'name':name_org, 'street_address':street_address_org,'place':place_org,'lat_org':lat_org,'lon_org':lon_org, 'display_name_org': name_org+', '+street_address_org+', '+place_org+', '+var_geo_name_org+', '+geo_name_literal_org}]
                    info_table=pd.DataFrame(info_table)
                    if lat != "None" and lon != "None":
                        address_name_search = geofabrik_url+'/reverse.php?lat=' + lat + '&lon=' + lon + '&zoom=18&format=jsonv2&addressdetails=1&extratags=1&class=amenity,[building],[leisure]]'
                        try:
                            webURL = urllib.request.urlopen(address_name_search)
                            data = webURL.read()
                            data_1 = json.loads(data.decode('utf-8'))
                            result= pd.json_normalize(data_1)
                        except urllib.error.URLError as e:
                            code=e.code                
                    
                    elif street_address != "None" and name != "None":
                        address_name_search = geofabrik_url+'/search.php?q=' + name + '%20' + street_address + '%20' + place + '&format=jsonv2&addressdetails=1&extratags=1&class=amenity,[building],[leisure]]'
                        try:
                            webURL = urllib.request.urlopen(address_name_search)
                            data = webURL.read()
                            data_1 = json.loads(data.decode('utf-8'))
                            result= pd.json_normalize(data_1)
                            if len(result) == 0:
                                address_name_search = geofabrik_url+'/search.php?q='+ name_place + '&format=jsonv2&addressdetails=1&extratags=1&class=amenity,[building],[leisure]]'
                                webURL = urllib.request.urlopen(address_name_search)
                                data = webURL.read()
                                data_1 = json.loads(data.decode('utf-8'))
                                result= pd.json_normalize(data_1)
                        except urllib.error.URLError as e:
                            code=e.code
                        
                    elif street_address != "None":
                        address_name_search = geofabrik_url+'/search.php?q=' + street_address + '%20' + place + '&format=jsonv2&addressdetails=1&extratags=1&class=amenity,[building],[leisure]]'
                        try:
                            webURL = urllib.request.urlopen(address_name_search)
                            data = webURL.read()
                            data_1 = json.loads(data.decode('utf-8'))
                            result= pd.json_normalize(data_1)
                        except urllib.error.URLError as e:
                            code=e.code               
                    
                    elif name != "None":
                        address_name_search = geofabrik_url+'/search.php?q=' + name_place + '&format=jsonv2&addressdetails=1&extratags=1&class=amenity,[building],[leisure]]'
                        try:
                            webURL = urllib.request.urlopen(address_name_search)
                            data = webURL.read()
                            data_1 = json.loads(data.decode('utf-8'))
                            result= pd.json_normalize(data_1)
                        except urllib.error.URLError as e:
                            code=e.code 

                    if save_status == 'Speichern':
                        result=result.head(1)
                        columns = ['place_id','lat','lon', 'display_name', 'name', 'address.historic','address.road','address.house_number', 'address.hamlet', 'address.town', 'address.city_district', 'address.city', 'address.county', 'address.state', 'address.ISO3166-2-lvl4', 'address.postcode', 'address.country', 'extratags.wikidata']
                        columns = [col for col in columns if col in result.columns]
                        df_enrichment = result[columns]

                        score_table_names = []
                        if 'display_name_org' in info_table and 'display_name' in df_enrichment:
                            info_table['display_name_org']=info_table['display_name_org'].astype(str)
                            info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                            info_table['display_name_org']=info_table['display_name_org'].map(lambda x: x.lower())
                            info_table['display_name_org']=info_table['display_name_org'].map(lambda x: x.strip())
                            info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[^\w\s]+', '')
                            info_table_1=info_table['display_name_org'].str.split(expand=True)
                            info_table_1=info_table_1.values.tolist()
                            filtered_info_table_list=[x for x in info_table_1[0] if "None" not in x]
                            filtered_info_table_list=[x for x in info_table_1[0] if "none" not in x]
                            df_enrichment['display_name'] = df_enrichment['display_name'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                            df_enrichment['display_name'] = df_enrichment['display_name'].astype(str)
                            df_enrichment['display_name'] = df_enrichment['display_name'].map(lambda x: x.lower())
                            df_enrichment['display_name'] = df_enrichment['display_name'].map(lambda x: x.strip())
                            df_enrichment_1=df_enrichment['display_name'].str.split(expand=True)
                            df_enrichment_1 =df_enrichment_1.values.tolist()

                            name_score=[]
                            name_scores=[]
                            distance_score=[]
                            distance_scores=[]
                            for items in df_enrichment_1:
                                filtered_list = list(filter(None, items))
                                list_0 = filtered_info_table_list[0:]
                                list_1 = list(set(list_0))
                                len_list_1 = len(list_1)
                                list_2 = list(set(filtered_list))
                                score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                    name_score= result1
                                    
                                elif 0 in score_matrix and 1 in score_matrix:
                                    count_0=(score_matrix==0).sum()
                                    result1 = count_0
                                    count_1=(score_matrix==1).sum()
                                    if count_1 == 1:
                                        result2=0.9
                                        name_score= result1 + result2
                                    elif count_1 > 1:
                                        result3=count_1*0.9
                                        name_score= result1 + result3
                                elif 1 in score_matrix:
                                    count_1=(score_matrix==1).sum()
                                    if count_1 == 1:
                                        result2=0.9
                                        name_score= result2
                                    elif count_1 > 1:
                                        result3=count_1*0.9
                                        name_score= result3
                                    else:
                                        result4=0
                                        name_score=result4                         
                                else:
                                    name_score = 0

                                if name_score == len_list_1:
                                    distance_score = 1
                                elif name_score == (len_list_1 - 1):
                                    distance_score = 0.75
                                elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:  
                                    distance_score = 0.50
                                elif name_score < (len_list_1/2) and name_score!=0:
                                    distance_score = 0.25
                                else:
                                    distance_score = 0
                                name='distance_score_geofabrik'
                                s=pd.Series(data=distance_score, name=name, dtype=float)
                                distance_scores.append(s)

                                name='lev_distance_geofabrik'
                                s = pd.Series(data=name_score, name=name, dtype=float)
                                name_scores.append(s)
                            name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                            name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                            
                            list_1 = str(', '.join(list_1))
                            name='display_name_org'
                            display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                            list_2 = str(', '.join(list_2))
                            name='display_name_target_geofabrik'
                            display_name_target = pd.Series(data=list_2, name=name, dtype=str)
                            score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1,name_score_table_2],axis=1)
                            score_table_names=pd.DataFrame(score_table_names)
                        city=""
                        dms = []
                        if 'lat' in df_enrichment and 'lon' in df_enrichment and lon_org != 'None' and lat_org != 'None':
                            geo_distance_score= ""
                            score_table=pd.concat([info_table['lat_org'], info_table['lon_org'],df_enrichment['lat'], df_enrichment['lon']], axis=1).astype(float)
                            score_table['distance'] = [haversine(score_table.lon_org[i],score_table.lat_org[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                            score_table['distance'] = score_table['distance'].round(decimals=1).astype(float)
                            geo_distance_score= np.where(((score_table['distance'].all() == 0) or (score_table['distance'].all() <= 2.0)), 1, 0)

                            name_place=name_org + ' ' +place_org
                            coords = [[name_place, float(lat_org), float(lon_org)]] 
                            for city,x,y in coords:
                                city=str(city)              
                                dms=dd2dms(x, y)
                                dms=pd.DataFrame(dms).transpose()
                                dms = dms.rename(columns={0: 'dms_lat', 1: 'dms_lon'})
                        else:
                            geo_distance_score=0                       
                            dms =pd.DataFrame(dms, columns=['dms_lat','dms_lon'])
                            dms['dms_lat'] = 'No lat'
                            dms['dms_lon'] = 'No lon'

                        geo_distance_score_table=pd.Series(data=geo_distance_score, name='geo_distance_score', dtype=str)
                        geo_distance_score_table=pd.DataFrame(geo_distance_score_table)

                        geo_distance_score_table=pd.concat([geo_distance_score_table, dms], axis=1, ignore_index=False)

                        if 'extratags.wikidata' in result and len(result.index) !=0:
                            wiki_id=""
                            for w_id in result['extratags.wikidata'].values:
                                w_id=str(w_id)
                                if w_id!='nan':
                                    wiki_id=w_id
                                    #st.info("wikidata_id:",str(wiki_id)) 
                                    wikidata_id=str(wiki_id)
                                    url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&ids=' + wikidata_id + '&format=json&language=en&type=item'
                                    webURL = urllib.request.urlopen(url)
                                    data = webURL.read()
                                    data_2 = json.loads(data.decode('utf-8'))
                                    result_2= pd.json_normalize(data_2)
                                    result_2=result_2.head(1)

                                if 'entities.'+wikidata_id+'.claims.P227' in result_2:
                                    column='entities.'+wikidata_id+'.claims.P227'
                                    gnd_id=pd.json_normalize(result_2[column])
                                    gnd_id=pd.json_normalize(gnd_id[0])

                                    if 'mainsnak.datavalue.value' in gnd_id:
                                        gnd_id=gnd_id['mainsnak.datavalue.value']
                                        gnd_id=str(gnd_id[0])
                                        time.sleep(0.5)
                                        url = 'http://lobid.org/gnd/' + gnd_id + '.json'
                                        webURL = urllib.request.urlopen(url)
                                        lobid_data = webURL.read()
                                        data_3 = json.loads(lobid_data.decode('utf-8'))
                                        result_3= pd.json_normalize(data_3)

                                        columns = ['gndIdentifier','preferredName', 'geographicAreaCode', 'placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                                        columns = [col for col in columns if col in result_3.columns]
                                        new_enrichment = result_3[columns]

                                    if 'preferredName' in new_enrichment:
                                        new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                        df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                        df_enrichment_1 =df_enrichment_1.values.tolist()

                                        name_score=[]
                                        name_scores=[]
                                        distance_score=[]
                                        distance_scores=[]
                                        for items in df_enrichment_1:
                                            filtered_list = list(filter(None, items))
                                            list_0 = filtered_info_table_list[0:]
                                            list_1 = list(set(list_0))
                                            len_list_1 = len(list_1)
                                            list_2 = list(set(filtered_list))
                                            score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                                name_score= result1
                                                
                                            elif 0 in score_matrix and 1 in score_matrix:
                                                count_0=(score_matrix==0).sum()
                                                result1 = count_0
                                                count_1=(score_matrix==1).sum()
                                                if count_1 == 1:
                                                    result2=0.9
                                                    name_score= result1 + result2
                                                elif count_1 > 1:
                                                    result3=count_1*0.9
                                                    name_score= result1 + result3
                                            elif 1 in score_matrix :
                                                count_1=(score_matrix==1).sum()
                                                if count_1 == 1:
                                                    result2=0.9
                                                    name_score= result2
                                                elif count_1 > 1:
                                                    result3=count_1*0.9
                                                    name_score= result3
                                                else:
                                                    result4=0
                                                    name_score=result4                         
                                            else:
                                                name_score = 0
                                            if name_score == len_list_1:
                                                distance_score = 1
                                            elif name_score == (len_list_1 - 1):
                                                distance_score = 0.75
                                            elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                                distance_score = 0.50
                                            elif name_score < (len_list_1/2) and name_score!=0:
                                                distance_score = 0.25
                                            else:
                                                distance_score = 0
                                            name='distance_score_lobid'
                                            s=pd.Series(data=distance_score, name=name, dtype=float)
                                            distance_scores.append(s)

                                            name='lev_distance_lobid'
                                            s = pd.Series(data=name_score, name=name, dtype=float)
                                            name_scores.append(s)
                                        name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                        name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                        
                                        list_1 = str(', '.join(list_1))
                                        name='display_name_org'
                                        display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                        display_name_target_nds =new_enrichment['preferredName']


                                        gnd_id_score=[]
                                        gnd_id_scores=[]
                                        gnd_id_lobid=""
                                        gnd_id_lobids=[]

                                        for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                            gnd_id_lobid=gnd_id_lobid
                                            if gnd_id_org == gnd_id_lobid:
                                                gnd_id_score= 1
                                            else:
                                                gnd_id_score= 0
                                            s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                            gnd_id_scores.append(s)
                                            gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                            gnd_id_lobids.append(gnd_id)
                                        gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                        gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)
                                        score_table_names['gnd_id_org']=gnd_id_org
                                        score_table_names =pd.concat([score_table_names,display_name_target_nds,name_score_table_1,name_score_table_2,  gnd_id_lobids, gnd_id_scores, geo_distance_score_table], axis=1, ignore_index=False) 
                                        score_table_names['lev_distance_geofabrik']=score_table_names['lev_distance_geofabrik'].fillna(method='ffill')
                                        score_table_names['distance_score_geofabrik']=score_table_names['distance_score_geofabrik'].fillna(method='ffill')
                                        score_table_names['gnd_id_org']=score_table_names['gnd_id_org'].fillna(method='ffill')
                                        score_table_names['geo_distance_score']=score_table_names['geo_distance_score'].fillna(method='ffill')
                                        score_table_names['dms_lat']=score_table_names['dms_lat'].fillna(method='ffill')
                                        score_table_names['dms_lon']=score_table_names['dms_lon'].fillna(method='ffill') 
                                        score_table_names['total_score'] = score_table_names['distance_score_geofabrik'].astype(float) +score_table_names['distance_score_lobid'].astype(float)+ score_table_names['gnd_id_score'].astype(float) + score_table_names['geo_distance_score'].astype(float)
                                                                    
                                    else:
                                        pass
                                elif 'entities.'+wikidata_id+'.claims.P227' not in result_2:
                                    name=row[2]
                                    place=row[3]

                                    name=str(name)
                                    for char in chars:
                                        name = name.replace(char,chars[char])

                                    place=str(place)
                                    for char in chars:
                                        place = place.replace(char,chars[char])

                                    name_place = str(name + ' ' + place)
                                    sample_converted=Convert(name_place)
                                    sample_converted = [num for num in sample_converted]
                                    number_of_words_1 = [len(sentence.split()) for sentence in sample_converted]
                                    number_of_words_1 =sum(number_of_words_1)
                                    np_lists = np.array(sample_converted)
                                    output=[]
                                    if number_of_words_1 == 1:
                                        output = sample_converted
                                    elif number_of_words_1 > 1:
                                        filt = []
                                        for i in range(0,len(sample_converted)):
                                            filt_i = len(np_lists[i]) >= 1
                                            filt.append(filt_i)
                                        new_list = list(np_lists[filt])
                                    output = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
                                    
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
                                        if number_of_words_1 > 1 and number_of_words > 1:
                                            if search_terms != name and search_terms != place:
                                                path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                                col_name="search_terms"
                                                s = pd.Series(data=path1, name=col_name, dtype=str)
                                                search_results.append(s)
                                                df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                                    df_results_preferred_names=pd.DataFrame(df_results_preferred_names, columns=['search_terms'])
                                    df_results_1 = df_results_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                                    df=[]
                                    query_results=[]
                                    query_id_result=[]
                                    for value in df_results_1['search_terms']:
                                        value =str(value)
                                        address_name_search = 'https://lobid.org/gnd/search?q='+ value + '&filter=%2B%28type%3APlaceOrGeographicName%29+%2B+NOT+%28type%3ATerritorialCorporateBodyOrAdministrativeUnit%29&size=20'
                                        time.sleep(0.5)
                                        try:
                                            webURL = urllib.request.urlopen(address_name_search)
                                            data = webURL.read()
                                            data_1 = json.loads(data.decode('utf-8'))
                                            query_id= pd.json_normalize(data_1)
                                            query_id_result.append(query_id)
                                            df_id=pd.concat(query_id_result, ignore_index=True)
                                            result= pd.json_normalize(data_1,'member')
                                            query_results.append(result)
                                            df=pd.concat(query_results, ignore_index=True)
                                        except urllib.error.URLError as e:
                                            code=e.code     
                                    columns=['id', 'totalItems']
                                    df_id = df_id[columns]
                                    df_id['totalItems']=df_id['totalItems'].astype(int)
                                    df_id=df_id.sort_values(by='totalItems', ascending=False)
                                    df_id['url'] = df_id['id'].str.replace("&format=json", "")

                                    df = df.drop_duplicates(subset=['gndIdentifier'],keep='first') 
                                    df=df.reset_index()
                                    columns = ['gndIdentifier','preferredName', 'geographicAreaCode', 'placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                                    columns = [col for col in columns if col in df.columns]
                                    new_enrichment = df[columns]
                                    if 'preferredName' in new_enrichment:
                                        new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                        df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                        df_enrichment_1 =df_enrichment_1.values.tolist()

                                        name_score=[]
                                        name_scores=[]
                                        distance_score=[]
                                        distance_scores=[]
                                        for items in df_enrichment_1:
                                            filtered_list = list(filter(None, items))
                                            list_0 = filtered_info_table_list[0:]
                                            list_1 = list(set(list_0))
                                            len_list_1 = len(list_1)
                                            list_2 = list(set(filtered_list))
                                            score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                                name_score= result1
                                                
                                            elif 0 in score_matrix and 1 in score_matrix:
                                                count_0=(score_matrix==0).sum()
                                                result1 = count_0
                                                count_1=(score_matrix==1).sum()
                                                if count_1 == 1:
                                                    result2=0.9
                                                    name_score= result1 + result2
                                                elif count_1 > 1:
                                                    result3=count_1*0.9
                                                    name_score= result1 + result3
                                            elif 1 in score_matrix :
                                                count_1=(score_matrix==1).sum()
                                                if count_1 == 1:
                                                    result2=0.9
                                                    name_score= result2
                                                elif count_1 > 1:
                                                    result3=count_1*0.9
                                                    name_score= result3
                                                else:
                                                    result4=0
                                                    name_score=result4                         
                                            else:
                                                name_score = 0
                                            if name_score == len_list_1:
                                                distance_score = 1
                                            elif name_score == (len_list_1 - 1):
                                                distance_score = 0.75
                                            elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                                distance_score = 0.50
                                            elif name_score < (len_list_1/2) and name_score!=0:
                                                distance_score = 0.25
                                            else:
                                                distance_score = 0
                                            name='distance_score_lobid'
                                            s=pd.Series(data=distance_score, name=name, dtype=float)
                                            distance_scores.append(s)

                                            name='lev_distance_lobid'
                                            s = pd.Series(data=name_score, name=name, dtype=float)
                                            name_scores.append(s)
                                        name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                        name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                        
                                        list_1 = str(', '.join(list_1))
                                        name='display_name_org'
                                        display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                        display_name_target_nds =new_enrichment['preferredName']


                                        gnd_id_score=[]
                                        gnd_id_scores=[]
                                        gnd_id_lobid=""
                                        gnd_id_lobids=[]

                                        for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                            gnd_id_lobid=gnd_id_lobid
                                            if gnd_id_org == gnd_id_lobid:
                                                gnd_id_score= 1
                                            else:
                                                gnd_id_score= 0
                                            s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                            gnd_id_scores.append(s)
                                            gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                            gnd_id_lobids.append(gnd_id)
                                        gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                        gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)
                                        score_table_names['gnd_id_org']=gnd_id_org
                                        score_table_names =pd.concat([score_table_names,display_name_target_nds,name_score_table_1,name_score_table_2,  gnd_id_lobids, gnd_id_scores, geo_distance_score_table], axis=1, ignore_index=False) 
                                        score_table_names['lev_distance_geofabrik']=score_table_names['lev_distance_geofabrik'].fillna(method='ffill')
                                        score_table_names['distance_score_geofabrik']=score_table_names['distance_score_geofabrik'].fillna(method='ffill')
                                        score_table_names['gnd_id_org']=score_table_names['gnd_id_org'].fillna(method='ffill')
                                        score_table_names['geo_distance_score']=score_table_names['geo_distance_score'].fillna(method='ffill')
                                        score_table_names['dms_lat'] =score_table_names['dms_lat'].fillna(method='ffill')
                                        score_table_names['dms_lon'] =score_table_names['dms_lon'].fillna(method='ffill') 
                                        score_table_names['total_score'] = score_table_names['distance_score_geofabrik'].astype(float) +score_table_names['distance_score_lobid'].astype(float)+ score_table_names['gnd_id_score'].astype(float) + score_table_names['geo_distance_score'].astype(float)
                                        info_table=pd.concat([info_table, meta_data], axis=1)
                                        info_table=pd.DataFrame(info_table).rename(columns={"id":"dc_identifier"})
                                        info_table['target_query_system'] =str('lobid_osm')
                                        
                                        conn=db_connect()
                                        info_table.to_sql(name='queried_private_building_data', con=conn, if_exists='append', chunksize = 5000, method='multi')

                                        all_scores_to_save=pd.concat([meta_data,score_table_names], axis=1)
                                        all_scores_to_save['date_of_query'] =all_scores_to_save['date_of_query'].fillna(method='ffill')
                                        all_scores_to_save['default_title'] =all_scores_to_save['default_title'].fillna(method='ffill')
                                        all_scores_to_save['alternative_title'] =all_scores_to_save['alternative_title'].fillna(method='ffill')
                                        all_scores_to_save['comment_content'] =all_scores_to_save['comment_content'].fillna(method='ffill')
                                        all_scores_to_save['display_name_org'] =all_scores_to_save['display_name_org'].fillna(method='ffill')
                                        all_scores_to_save['source_data_id']=info_table['dc_identifier']
                                        all_scores_to_save['source_data_id']=all_scores_to_save['source_data_id'].fillna(method='ffill')
                                        all_scores_to_save['target_query_system'] =str('lobid_osm')
                                        
                                        match_status =" "
                                        comment = " "
                                        all_scores_to_save['match_status']=match_status
                                        all_scores_to_save['comment']=comment
                                        
                                        all_scores_to_save = all_scores_to_save.applymap(str)
                                        all_scores_to_save.to_sql(name='private_building_data_results', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi') 
                                    
                        elif 'extratags.wikidata' not in result:
                            gnd_id=row[1]
                            gnd_id_org=str(gnd_id)
                            name=row[2]
                            place=row[3]
                            name=str(name)
                            for char in chars:
                                name = name.replace(char,chars[char])
                            place=str(place)
                            for char in chars:
                                place = place.replace(char,chars[char])

                            name_place = str(name + ' ' + place)
                            sample_converted=Convert(name_place)
                            sample_converted = [num for num in sample_converted]
                            number_of_words_1 = [len(sentence.split()) for sentence in sample_converted]
                            number_of_words_1 =sum(number_of_words_1)
                            np_lists = np.array(sample_converted)
                            output=[]
                            if number_of_words_1 == 1:
                                output = sample_converted
                            elif number_of_words_1 > 1:
                                filt = []
                                for i in range(0,len(sample_converted)):
                                    filt_i = len(np_lists[i]) >= 1
                                    filt.append(filt_i)
                                new_list = list(np_lists[filt])
                            output = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
                            
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

                                if number_of_words_1 > 1 and number_of_words > 1:
                                    if search_terms != name and search_terms != place:
                                        path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                        col_name="search_terms"
                                        s = pd.Series(data=path1, name=col_name, dtype=str)
                                        search_results.append(s)
                                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                            df_results_preferred_names=pd.DataFrame(df_results_preferred_names, columns=['search_terms'])
                            df_results_1 = df_results_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                            df=[]
                            query_results=[]
                            query_id_result=[]
                            for value in df_results_1['search_terms']:
                                value =str(value)
                                address_name_search = 'https://lobid.org/gnd/search?q='+ value + '&filter=%2B%28type%3APlaceOrGeographicName%29+%2B+NOT+%28type%3ATerritorialCorporateBodyOrAdministrativeUnit%29&size=20'
                                time.sleep(0.5)
                                try:
                                    webURL = urllib.request.urlopen(address_name_search)
                                    data = webURL.read()
                                    data_1 = json.loads(data.decode('utf-8'))
                                    
                                    query_id= pd.json_normalize(data_1)
                                    query_id_result.append(query_id)
                                    df_id=pd.concat(query_id_result, ignore_index=True)
                        
                                    result= pd.json_normalize(data_1,'member')
                                    query_results.append(result)
                                    df=pd.concat(query_results, ignore_index=True)
                                except urllib.error.URLError as e:
                                    code=e.code       
                            
                            columns=['id', 'totalItems']
                            df_id = df_id[columns]
                            df_id['totalItems']=df_id['totalItems'].astype(int)
                            df_id=df_id.sort_values(by='totalItems', ascending=False)
                            df_id['url'] = df_id['id'].str.replace("&format=json", "")
                            df = df.drop_duplicates(subset=['gndIdentifier'],keep='first')

                            df=df.reset_index()
                            columns = ['gndIdentifier','preferredName', 'geographicAreaCode', 'placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                            columns = [col for col in columns if col in df.columns]
                            new_enrichment = df[columns]
                            if 'preferredName' in new_enrichment:
                                info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[^\w\s]+', '')
                                info_table_1=info_table['display_name_org'].str.split(expand=True)
                                info_table_1=info_table_1.values.tolist()
                                filtered_info_table_list=[x for x in info_table_1[0] if "None" not in x]                    
                                new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                df_enrichment_1 =df_enrichment_1.values.tolist()

                                name_score=[]
                                name_scores=[]
                                distance_score=[] 
                                distance_scores=[]
                                for items in df_enrichment_1:
                                    filtered_list = list(filter(None, items))
                                    list_0 = filtered_info_table_list[0:]
                                    list_1 = list(set(list_0))
                                    
                                    len_list_1 = len(list_1)
                                    list_2 = list(set(filtered_list))
                                    score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                        name_score= result1
                                        
                                    elif 0 in score_matrix and 1 in score_matrix:
                                        count_0=(score_matrix==0).sum()
                                        result1 = count_0
                                        count_1=(score_matrix==1).sum()
                                        if count_1 == 1:
                                            result2=0.9
                                            name_score= result1 + result2
                                        elif count_1 > 1:
                                            result3=count_1*0.9
                                            name_score= result1 + result3
                                    elif 1 in score_matrix :
                                        count_1=(score_matrix==1).sum()
                                        if count_1 == 1:
                                            result2=0.9
                                            name_score= result2
                                        elif count_1 > 1:
                                            result3=count_1*0.9
                                            name_score= result3
                                        else:
                                            result4=0
                                            name_score=result4                         
                                    else:
                                        name_score = 0
                                    if name_score == len_list_1:
                                        distance_score = 1
                                    elif name_score == (len_list_1 - 1):
                                        distance_score = 0.75
                                    elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                        distance_score = 0.50
                                    elif name_score < (len_list_1/2) and name_score!=0:
                                        distance_score = 0.25
                                    else:
                                        distance_score = 0
                                    name='distance_score_lobid'
                                    s=pd.Series(data=distance_score, name=name, dtype=float)
                                    distance_scores.append(s)

                                    name='lev_distance_lobid'
                                    s = pd.Series(data=name_score, name=name, dtype=float)
                                    name_scores.append(s)
                                name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                
                                list_1 = str(', '.join(list_1))
                                name='display_name_org'
                                display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                display_name_target =new_enrichment['preferredName']
                                score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1,name_score_table_2],axis=1, ignore_index=False)
                                score_table_names=pd.DataFrame(score_table_names)

                                gnd_id_score=[]
                                gnd_id_scores=[]
                                gnd_id_lobid=""
                                gnd_id_lobids=[]

                                for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                    gnd_id_lobid=gnd_id_lobid
                                    if gnd_id_org == gnd_id_lobid:
                                        gnd_id_score= 1
                                    else:
                                        gnd_id_score= 0
                                    s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                    gnd_id_scores.append(s)
                                    gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                    gnd_id_lobids.append(gnd_id)
                                gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)

                                if 'display_name_org' in score_table_names:
                                    score_tables= pd.concat([score_table_names, gnd_id_lobids, gnd_id_scores, geo_distance_score_table], axis=1)
                                    score_tables=pd.DataFrame(score_tables)
                                    score_tables['dms_lat'] =score_tables['dms_lat'].fillna(method='ffill')
                                    score_tables['dms_lon'] =score_tables['dms_lon'].fillna(method='ffill') 
                                    score_tables['total_score'] = score_tables['distance_score_lobid'].astype(float)+score_tables['gnd_id_score'].astype(float)
                                else:
                                    pass
                                info_table=pd.concat([info_table, meta_data], axis=1)
                                info_table=pd.DataFrame(info_table).rename(columns={"id":"dc_identifier"})
                                info_table['target_query_system'] =str('lobid_osm')
                                
                                conn=db_connect()
                                info_table.to_sql(name='queried_private_building_data', con=conn, if_exists='append', chunksize = 5000, method='multi')
                                
                                all_scores_to_save=pd.concat([meta_data,score_tables], axis=1)
                                all_scores_to_save['date_of_query'] =all_scores_to_save['date_of_query'].fillna(method='ffill')
                                all_scores_to_save['default_title'] =all_scores_to_save['default_title'].fillna(method='ffill')
                                all_scores_to_save['alternative_title'] =all_scores_to_save['alternative_title'].fillna(method='ffill')
                                all_scores_to_save['comment_content'] =all_scores_to_save['comment_content'].fillna(method='ffill')
                                all_scores_to_save['display_name_org'] =all_scores_to_save['display_name_org'].fillna(method='ffill')
                                all_scores_to_save['source_data_id']=info_table['dc_identifier']
                                all_scores_to_save['source_data_id']=all_scores_to_save['source_data_id'].fillna(method='ffill')
                                all_scores_to_save['target_query_system'] =str('lobid_osm')
                                
                                match_status =" "
                                comment = " "
                                all_scores_to_save['match_status']=match_status
                                all_scores_to_save['comment']=comment
                                
                                all_scores_to_save = all_scores_to_save.applymap(str)
                                all_scores_to_save.to_sql(name='private_building_data_results', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')                
                    

                    elif save_status == 'Nicht speichern':                
                        with st.expander("id: "+id + " " + name_1 + " " + place_1):
                            result=pd.DataFrame(result)
                            st.write("Query:", address_name_search)
                            result=result.head(1)
                            st.info("Data from Geofabrik:")
                            st.write(result)
                            columns = ['place_id','lat','lon', 'display_name', 'name', 'address.historic','address.road','address.house_number', 'address.hamlet', 'address.town', 'address.city_district', 'address.city', 'address.county', 'address.state', 'address.ISO3166-2-lvl4', 'address.postcode', 'address.country', 'extratags.wikidata']
                            columns = [col for col in columns if col in result.columns]
                            df_enrichment = result[columns]

                            score_table_names = []
                            if 'display_name_org' in info_table and 'display_name' in df_enrichment:
                                info_table['display_name_org']=info_table['display_name_org'].astype(str)
                                info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                                info_table['display_name_org']=info_table['display_name_org'].map(lambda x: x.lower())
                                info_table['display_name_org']=info_table['display_name_org'].map(lambda x: x.strip())
                                info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[^\w\s]+', '')
                                info_table_1=info_table['display_name_org'].str.split(expand=True)
                                info_table_1=info_table_1.values.tolist()
                                filtered_info_table_list=[x for x in info_table_1[0] if "None" not in x]
                                filtered_info_table_list=[x for x in info_table_1[0] if "none" not in x]
                                df_enrichment['display_name'] = df_enrichment['display_name'].str.replace(r'[{}]'.format(string.punctuation), ' ', regex=True)
                                df_enrichment['display_name'] = df_enrichment['display_name'].astype(str)
                                df_enrichment['display_name'] = df_enrichment['display_name'].map(lambda x: x.lower())
                                df_enrichment['display_name'] = df_enrichment['display_name'].map(lambda x: x.strip())
                                df_enrichment_1=df_enrichment['display_name'].str.split(expand=True)
                                df_enrichment_1 =df_enrichment_1.values.tolist()

                                name_score=[]
                                name_scores=[]
                                distance_score=[]
                                distance_scores=[]
                                for items in df_enrichment_1:
                                    filtered_list = list(filter(None, items))
                                    list_0 = filtered_info_table_list[0:]
                                    list_1 = list(set(list_0))
                                    len_list_1 = len(list_1)
                                    list_2 = list(set(filtered_list))
                                    score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                        name_score= result1
                                        
                                    elif 0 in score_matrix and 1 in score_matrix:
                                        count_0=(score_matrix==0).sum()
                                        result1 = count_0
                                        count_1=(score_matrix==1).sum()
                                        if count_1 == 1:
                                            result2=0.9
                                            name_score= result1 + result2
                                        elif count_1 > 1:
                                            result3=count_1*0.9
                                            name_score= result1 + result3
                                    elif 1 in score_matrix:
                                        count_1=(score_matrix==1).sum()
                                        if count_1 == 1:
                                            result2=0.9
                                            name_score= result2
                                        elif count_1 > 1:
                                            result3=count_1*0.9
                                            name_score= result3
                                        else:
                                            result4=0
                                            name_score=result4                         
                                    else:
                                        name_score = 0

                                    if name_score == len_list_1:
                                        distance_score = 1
                                    elif name_score == (len_list_1 - 1):
                                        distance_score = 0.75
                                    elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:  
                                        distance_score = 0.50
                                    elif name_score < (len_list_1/2) and name_score!=0:
                                        distance_score = 0.25
                                    else:
                                        distance_score = 0
                                    name='distance_score_geofabrik'
                                    s=pd.Series(data=distance_score, name=name, dtype=float)
                                    distance_scores.append(s)

                                    name='lev_distance_geofabrik'
                                    s = pd.Series(data=name_score, name=name, dtype=float)
                                    name_scores.append(s)
                                name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                
                                list_1 = str(', '.join(list_1))
                                name='display_name_org'
                                display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                list_2 = str(', '.join(list_2))
                                name='display_name_target_geofabrik'
                                display_name_target = pd.Series(data=list_2, name=name, dtype=str)
                                score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1,name_score_table_2],axis=1)
                                score_table_names=pd.DataFrame(score_table_names)
                            city=""
                            dms = []

                            if 'lat' in df_enrichment and 'lon' in df_enrichment:
                                geo_fab_lat=df_enrichment['lat']
                                geo_fab_lat=pd.DataFrame(geo_fab_lat)
                                geo_fab_lon=df_enrichment['lon']
                                geo_fab_lon=pd.DataFrame(geo_fab_lon)
                                geo_fab_lat = geo_fab_lat.rename(columns={'lat':'geofab_lat'})
                                geo_fab_lon = geo_fab_lon.rename(columns={'lon':'geofab_lon'})

                            else:
                                geo_fab_lat='none'
                                geo_fab_lon='none'
                            if 'lat' in df_enrichment and 'lon' in df_enrichment and lon_org != 'None' and lat_org != 'None':
                                geo_distance_score= ""
                                score_table=pd.concat([info_table['lat_org'], info_table['lon_org'],df_enrichment['lat'], df_enrichment['lon']], axis=1).astype(float)
                                score_table['distance'] = [haversine(score_table.lon_org[i],score_table.lat_org[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                score_table['distance'] = score_table['distance'].round(decimals=1).astype(float)
                                geo_distance_score= np.where(((score_table['distance'].all() == 0) or (score_table['distance'].all() <= 2.0)), 1, 0)

                                name_place=name_org + ' ' +place_org
                                coords = [[name_place, float(lat_org), float(lon_org)]] 
                                for city,x,y in coords:
                                    city=str(city)              
                                    dms=dd2dms(x, y)
                                    dms=pd.DataFrame(dms).transpose()
                                    dms = dms.rename(columns={0: 'dms_lat', 1: 'dms_lon'})
                            else:
                                geo_distance_score=0                       
                                dms =pd.DataFrame(dms, columns=['dms_lat','dms_lon'])
                                dms['dms_lat'] = 'No lat'
                                dms['dms_lon'] = 'No lon'
                            st.info("Conversion from Degree Decimal (DD) to 'Analog' (DMS)")
                            st.write("Name of the City: ",city)
                            st.write(dms)

                            geo_distance_score_table=pd.Series(data=geo_distance_score, name='geo_distance_score', dtype=str)
                            geo_distance_score_table=pd.DataFrame(geo_distance_score_table)

                            geo_distance_score_table=pd.concat([geo_distance_score_table, dms], axis=1, ignore_index=False)

                            if 'geo_distance_score' in geo_distance_score_table and lon_org != 'None' and lat_org != 'None':
                                st.info("Comparison based on Geofabrik")
                                id_1=uuid.uuid4()
                                id_2=uuid.uuid4()
                                key_id_1= str(id_1)
                                key_id_2= str(id_2)

                                col1, col2 = st.columns([2,2])
                                with col1:
                                    st.info("Source Data_1")
                                    st.write(info_table)
                                    m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                    folium.Marker(
                                        [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                    ).add_to(m)
                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                with col2:
                                    st.info("Target Data")
                                    st.write(df_enrichment)
                                    m = folium.Map(location=[df_enrichment.lat, df_enrichment.lon], zoom_start=16)
                                    folium.Marker(
                                        [df_enrichment.lat, df_enrichment.lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                    ).add_to(m)
                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)

                            if 'extratags.wikidata' in result and len(result.index) !=0:
                                wiki_id=""
                                for w_id in result['extratags.wikidata'].values:
                                    w_id=str(w_id)
                                    if w_id!='nan':
                                        wiki_id=w_id
                                        wikidata_id=str(wiki_id)
                                        url = 'https://www.wikidata.org/w/api.php?action=wbgetentities&ids=' + wikidata_id + '&format=json&language=en&type=item'
                                        webURL = urllib.request.urlopen(url)
                                        data = webURL.read()
                                        data_2 = json.loads(data.decode('utf-8'))
                                        result_2= pd.json_normalize(data_2)
                                        result_2=result_2.head(1)
                                        st.info("Data from Wikidata.")
                                        st.write(result_2)
                                    if 'entities.'+wikidata_id+'.claims.P227' in result_2:
                                        column='entities.'+wikidata_id+'.claims.P227'
                                        gnd_id=pd.json_normalize(result_2[column])
                                        gnd_id=pd.json_normalize(gnd_id[0])

                                        if 'mainsnak.datavalue.value' in gnd_id:
                                            gnd_id=gnd_id['mainsnak.datavalue.value']
                                            gnd_id=str(gnd_id[0])
                                            time.sleep(0.5)
                                            url = 'http://lobid.org/gnd/' + gnd_id + '.json'
                                            webURL = urllib.request.urlopen(url)
                                            lobid_data = webURL.read()
                                            data_3 = json.loads(lobid_data.decode('utf-8'))
                                            result_3= pd.json_normalize(data_3)
                                            columns = ['gndIdentifier','preferredName', 'geographicAreaCode','hasGeometry','placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                                            columns = [col for col in columns if col in result_3.columns]
                                            new_enrichment = result_3[columns]
                                        score_table=[]
                                        if 'place' in new_enrichment and lon_org != 'None' and lat_org != 'None':
                                            new_enrichment['place']=new_enrichment['place'].astype(str)
                                            new_enrichment['place']=new_enrichment['place'].apply(lambda st: st[st.find("gnd/"):st.find("',")])
                                            new_enrichment['place']=new_enrichment['place'].str.replace("gnd/",'')
                                            new_enrichment['place_lenght']= new_enrichment['place'].str.len()
                                            new_enrichment['place']=np.where((new_enrichment['place_lenght'] == 0),'No Place', new_enrichment['place'])
                                            query_results=[]
                                            for id in new_enrichment['place']:
                                                if id == 'No Place':
                                                    birth_place_data="No Place"
                                                    name="hasGeometry"
                                                    s = pd.Series(data=birth_place_data, name=name, dtype=str)
                                                    query_results.append(s)
                                                else:
                                                    url = 'http://lobid.org/gnd/' + id + '.json'
                                                    webURL = urllib.request.urlopen(url)
                                                    lobid_data = webURL.read()
                                                    data_4 = json.loads(lobid_data.decode('utf-8'))
                                                    result_4= pd.json_normalize(data_4)
                                                    query_results.append(result_4)
                                            df=pd.concat(query_results, ignore_index=True)

                                            df['hasGeometry'] = df['hasGeometry'].astype(str)
                                            df['lat_lon']=df['hasGeometry'].apply(lambda st: st[st.find("( +"):st.find(" )")])
                                            df['lat_lon']=df['lat_lon'].str.replace("(",'')
                                            df['lat_lon']=df['lat_lon'].str.replace("+",'')
                                            df['lat_lon']=df['lat_lon'].str.lstrip()
                                            df['lat_lon_lenght']=df['lat_lon'].str.len()
                                            df['lat_lon']=np.where((df['lat_lon_lenght'] != 0), df['lat_lon'], '0 0')
                                            df[['lon', 'lat']] = df['lat_lon'].str.split(' ', expand=True)

                                            score_table=pd.concat([geo_fab_lat['geofab_lat'], geo_fab_lon['geofab_lon'],df['lat'], df['lon']], axis=1).astype(float)
                                            score_table['geofab_lat']=score_table['geofab_lat'].fillna(method='ffill')
                                            score_table['geofab_lon']=score_table['geofab_lon'].fillna(method='ffill')
                                            score_table['distance'] = [haversine(score_table.geofab_lon[i],score_table.geofab_lat[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                        
                                            score_table['distance'] = score_table['distance'].round(decimals=1).astype(str)
                                            score_table=pd.DataFrame(score_table).astype(str)
                                            score_table['geo_distance_score_lobid']= np.where(((score_table['distance'] <='10.0')), 1, 0)
                                            new_enrichment=pd.concat([new_enrichment, score_table,geo_distance_score_table], axis=1)

                                        else:
                                            score_table = ['0']
                                            score_table=pd.DataFrame(score_table, columns=['geo_distance_score_lobid'])
                                            new_enrichment=pd.concat([new_enrichment, score_table], axis=1)
                                            new_enrichment['geo_distance_score_lobid']=new_enrichment['geo_distance_score_lobid'].fillna(method='ffill')
                                            score_table=new_enrichment

                                        for score in score_table['geo_distance_score_lobid']:

                                            if score == 1 and lon_org != 'None' and lat_org != 'None':
                                                st.info("Comparison based on Lobid")
                                                id_1=uuid.uuid4()
                                                key_id_1= str(id_1)
                                                col1, col2 = st.columns([2,2])
                                                with col1:
                                                    st.info("Source Data")
                                                    st.write(info_table)
                                                    m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                                    folium.Marker(
                                                        [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                                    ).add_to(m)
                                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                                with col2:
                                                    st.info("Target Data")
                                                    lon_lat =new_enrichment[new_enrichment['geo_distance_score_lobid'] ==1]
                                                    lon_lat =lon_lat.reset_index()
                                                    st.write(lon_lat)
                                                    for lat in lon_lat['lat']:
                                                        for lon in lon_lat['lon']:
                                                            lon = lon
                                                            lat=lat
                                                            id_2=uuid.uuid4()
                                                            key_id_2= str(id_2)
                                                        m = folium.Map(location=[lat, lon], zoom_start=16)
                                                        folium.Marker(
                                                            [lat, lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                                        ).add_to(m)
                                                        st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                                            else:
                                                pass

                                        if 'preferredName' in new_enrichment:
                                            st.info("Data from Lobid")
                                            st.write(new_enrichment.astype(str)) 

                                            new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                            df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                            df_enrichment_1 =df_enrichment_1.values.tolist()

                                            name_score=[]
                                            name_scores=[]
                                            distance_score=[]
                                            distance_scores=[]
                                            for items in df_enrichment_1:
                                                filtered_list = list(filter(None, items))
                                                list_0 = filtered_info_table_list[0:]
                                                list_1 = list(set(list_0))
                                                len_list_1 = len(list_1)
                                                list_2 = list(set(filtered_list))
                                                score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                                    name_score= result1
                                                    
                                                elif 0 in score_matrix and 1 in score_matrix:
                                                    count_0=(score_matrix==0).sum()
                                                    result1 = count_0
                                                    count_1=(score_matrix==1).sum()
                                                    if count_1 == 1:
                                                        result2=0.9
                                                        name_score= result1 + result2
                                                    elif count_1 > 1:
                                                        result3=count_1*0.9
                                                        name_score= result1 + result3
                                                elif 1 in score_matrix :
                                                    count_1=(score_matrix==1).sum()
                                                    if count_1 == 1:
                                                        result2=0.9
                                                        name_score= result2
                                                    elif count_1 > 1:
                                                        result3=count_1*0.9
                                                        name_score= result3
                                                    else:
                                                        result4=0
                                                        name_score=result4                         
                                                else:
                                                    name_score = 0
                                                if name_score == len_list_1:
                                                    distance_score = 1
                                                elif name_score == (len_list_1 - 1):
                                                    distance_score = 0.75
                                                elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                                    distance_score = 0.50
                                                elif name_score < (len_list_1/2) and name_score!=0:
                                                    distance_score = 0.25
                                                else:
                                                    distance_score = 0
                                                name='distance_score_lobid'
                                                s=pd.Series(data=distance_score, name=name, dtype=float)
                                                distance_scores.append(s)

                                                name='lev_distance_lobid'
                                                s = pd.Series(data=name_score, name=name, dtype=float)
                                                name_scores.append(s)
                                            name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                            name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                            
                                            list_1 = str(', '.join(list_1))
                                            name='display_name_org'
                                            display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                            display_name_target_nds =new_enrichment['preferredName']

                                            gnd_id_score=[]
                                            gnd_id_scores=[]
                                            gnd_id_lobid=""
                                            gnd_id_lobids=[]

                                            for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                                gnd_id_lobid=gnd_id_lobid
                                                if gnd_id_org == gnd_id_lobid:
                                                    gnd_id_score= 1
                                                else:
                                                    gnd_id_score= 0
                                                s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                                gnd_id_scores.append(s)
                                                gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                                gnd_id_lobids.append(gnd_id)
                                            gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                            gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)
                                            score_table_names['gnd_id_org']=gnd_id_org
                                            score_table_names =pd.concat([score_table_names,display_name_target_nds,name_score_table_1,name_score_table_2,  gnd_id_lobids, gnd_id_scores, geo_distance_score_table, score_table['geo_distance_score_lobid']], axis=1, ignore_index=False) 
                                            score_table_names['lev_distance_geofabrik']=score_table_names['lev_distance_geofabrik'].fillna(method='ffill')
                                            score_table_names['distance_score_geofabrik']=score_table_names['distance_score_geofabrik'].fillna(method='ffill')
                                            score_table_names['gnd_id_org']=score_table_names['gnd_id_org'].fillna(method='ffill')
                                            score_table_names['geo_distance_score']=score_table_names['geo_distance_score'].fillna(method='ffill')
                                            score_table_names['dms_lat']=score_table_names['dms_lat'].fillna(method='ffill')
                                            score_table_names['dms_lon']=score_table_names['dms_lon'].fillna(method='ffill') 
                                            score_table_names['total_score'] = score_table_names['distance_score_geofabrik'].astype(float) +score_table_names['distance_score_lobid'].astype(float)+ score_table_names['gnd_id_score'].astype(float) + score_table_names['geo_distance_score'].astype(float)+ score_table_names['geo_distance_score_lobid'].astype(float)
                                                                        
                                            st.info("Total Score Table")
                                            st.write(score_table_names)
                                        else:
                                            pass
                                    elif 'entities.'+wikidata_id+'.claims.P227' not in result_2:
                                        name=name_org
                                        place=place_org

                                        name=str(name)
                                        for char in chars:
                                            name = name.replace(char,chars[char])
                                        place=str(place)
                                        for char in chars:
                                            place = place.replace(char,chars[char])

                                        name_place = str(name + ' ' + place)
                                        name_place = name_place.replace('&', '')
                                        pattern = r'[' + string.punctuation + ']'
                                        name_place = re.sub(pattern, ' ', name_place)
                                        name_place= ''.join(i for i in name_place if not i.isdigit())
                                        sample_converted=Convert(name_place)
                                        sample_converted = [num for num in sample_converted]
                                        number_of_words_1 = [len(sentence.split()) for sentence in sample_converted]
                                        number_of_words_1 =sum(number_of_words_1)
                                        np_lists = np.array(sample_converted)
                                        output=[]
                                        if number_of_words_1 == 1:
                                            output = sample_converted
                                        elif number_of_words_1 > 1:
                                            filt = []
                                            for i in range(0,len(sample_converted)):
                                                filt_i = len(np_lists[i]) >= 1
                                                filt.append(filt_i)
                                            new_list = list(np_lists[filt])
                                        output = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
                                        
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
                                            if number_of_words_1 == 1 and number_of_words ==1:
                                                path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                                col_name="search_terms"
                                                s = pd.Series(data=path1, name=col_name, dtype=str)
                                                search_results.append(s)
                                                df_results_preferred_names=pd.concat(search_results, ignore_index=True)

                                            elif number_of_words_1 > 1 and number_of_words > 1:
                                                if search_terms != name and search_terms != place:
                                                    path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                                    col_name="search_terms"
                                                    s = pd.Series(data=path1, name=col_name, dtype=str)
                                                    search_results.append(s)
                                                    df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                                        df_results_preferred_names=pd.DataFrame(df_results_preferred_names, columns=['search_terms'])
                                        df_results_1 = df_results_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                                        df=[]
                                        query_results=[]
                                        query_id_result=[]
                                        for value in df_results_1['search_terms']:
                                            value =str(value)
                                            address_name_search = 'https://lobid.org/gnd/search?q='+ value + '&filter=%2B%28type%3APlaceOrGeographicName%29+%2B+NOT+%28type%3ATerritorialCorporateBodyOrAdministrativeUnit%29&size=20'
                                            time.sleep(0.5)
                                            try:
                                                webURL = urllib.request.urlopen(address_name_search)
                                                data = webURL.read()
                                                data_1 = json.loads(data.decode('utf-8'))
                                                query_id= pd.json_normalize(data_1)
                                                query_id_result.append(query_id)
                                                df_id=pd.concat(query_id_result, ignore_index=True)
                                                result= pd.json_normalize(data_1,'member')
                                                query_results.append(result)
                                                df=pd.concat(query_results, ignore_index=True)
                                            except urllib.error.URLError as e:
                                                code=e.code     
                                        columns=['id', 'totalItems']
                                        df_id = df_id[columns]
                                        df_id['totalItems']=df_id['totalItems'].astype(int)
                                        df_id=df_id.sort_values(by='totalItems', ascending=False)
                                        df_id['url'] = df_id['id'].str.replace("&format=json", "")
                                        gd = GridOptionsBuilder.from_dataframe(df_id)
                                        gd.configure_pagination(enabled=True)
                                        gd.configure_default_column(editable=True, groupable=True)
                                        gd.configure_column("url",
                                                            headerName="query_url",
                                                            cellRenderer=JsCode("""
                                                        function(params) {
                                                            return '<a href=' + params.value + ' target="_blank">🖱️</a>'
                                                            }
                                                        """))
                                        gd.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True)
                                        gridoptions = gd.build()
                                        st.info("Lobid Query Sets XX")
                                        grid_table=AgGrid(df_id, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True, key=id)

                                        df = df.drop_duplicates(subset=['gndIdentifier'],keep='first') 
                                        df=df.reset_index()
                                        columns = ['gndIdentifier','preferredName', 'geographicAreaCode', 'hasGeometry','placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                                        columns = [col for col in columns if col in df.columns]
                                        new_enrichment = df[columns]

                                        score_table=[]
                                        if 'place' in new_enrichment and lon_org != 'None' and lat_org != 'None':
                                            new_enrichment['place']=new_enrichment['place'].astype(str)
                                            new_enrichment['place']=new_enrichment['place'].apply(lambda st: st[st.find("gnd/"):st.find("',")])
                                            new_enrichment['place']=new_enrichment['place'].str.replace("gnd/",'')
                                            new_enrichment['place_lenght']= new_enrichment['place'].str.len()
                                            new_enrichment['place']=np.where((new_enrichment['place_lenght'] == 0),'No Place', new_enrichment['place'])
                                            query_results=[]
                                            for id in new_enrichment['place']:
                                                if id == 'No Place':
                                                    birth_place_data="No Place"
                                                    name="hasGeometry"
                                                    s = pd.Series(data=birth_place_data, name=name, dtype=str)
                                                    query_results.append(s)
                                                else:
                                                    url = 'http://lobid.org/gnd/' + id + '.json'
                                                    webURL = urllib.request.urlopen(url)
                                                    lobid_data = webURL.read()
                                                    data_4 = json.loads(lobid_data.decode('utf-8'))
                                                    result_4= pd.json_normalize(data_4)
                                                    query_results.append(result_4)
                                            df=pd.concat(query_results, ignore_index=True)
                                            df['hasGeometry'] = df['hasGeometry'].astype(str)
                                            df['lat_lon']=df['hasGeometry'].apply(lambda st: st[st.find("( +"):st.find(" )")])
                                            df['lat_lon']=df['lat_lon'].str.replace("(",'')
                                            df['lat_lon']=df['lat_lon'].str.replace("+",'')
                                            df['lat_lon']=df['lat_lon'].str.lstrip()
                                            df['lat_lon_lenght']=df['lat_lon'].str.len()
                                            df['lat_lon']=np.where((df['lat_lon_lenght'] != 0), df['lat_lon'], '0 0')
                                            df[['lon', 'lat']] = df['lat_lon'].str.split(' ', expand=True)

                                            score_table=pd.concat([geo_fab_lat['geofab_lat'], geo_fab_lon['geofab_lon'],df['lat'], df['lon']], axis=1).astype(float)
                                            score_table['geofab_lat']=score_table['geofab_lat'].fillna(method='ffill')
                                            score_table['geofab_lon']=score_table['geofab_lon'].fillna(method='ffill')
                                            score_table['distance'] = [haversine(score_table.geofab_lon[i],score_table.geofab_lat[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                        
                                            score_table['distance'] = score_table['distance'].round(decimals=1).astype(str)
                                            score_table=pd.DataFrame(score_table).astype(str)
                                            score_table['geo_distance_score_lobid']= np.where(((score_table['distance'] <='10.0')), 1, 0)
                                            new_enrichment=pd.concat([new_enrichment, score_table,geo_distance_score_table], axis=1)

                                        else:
                                            score_table = ['0']
                                            score_table=pd.DataFrame(score_table, columns=['geo_distance_score_lobid'])
                                            new_enrichment=pd.concat([new_enrichment, score_table], axis=1)
                                            new_enrichment['geo_distance_score_lobid']=new_enrichment['geo_distance_score_lobid'].fillna(method='ffill')
                                            score_table=new_enrichment
                                            score_table=pd.concat([display_name_org, score_table], axis=1)
                                            score_table['total_score'] = score_table['geo_distance_score_lobid'].astype(float)
                                            st.info("Total Score Table")
                                            st.write(score_table.astype(str)) 
                                        for score in score_table['geo_distance_score_lobid']:
                                            if score == 1 and lon_org != 'None' and lat_org != 'None':
                                                st.info("Comparison based on Lobid")
                                                id_1=uuid.uuid4()
                                                key_id_1= str(id_1)
                                                col1, col2 = st.columns([2,2])
                                                with col1:
                                                    st.info("Source Data")
                                                    st.write(info_table)
                                                    m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                                    folium.Marker(
                                                        [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                                    ).add_to(m)
                                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                                with col2:
                                                    st.info("Target Data")
                                                    lon_lat =new_enrichment[new_enrichment['geo_distance_score_lobid'] ==1]
                                                    lon_lat =lon_lat.reset_index()
                                                    st.write(lon_lat)
                                                    for lat in lon_lat['lat']:
                                                        for lon in lon_lat['lon']:
                                                            lon = lon
                                                            lat=lat
                                                            id_2=uuid.uuid4()
                                                            key_id_2= str(id_2)
                                                        m = folium.Map(location=[lat, lon], zoom_start=16)
                                                        folium.Marker(
                                                            [lat, lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                                        ).add_to(m)
                                                        st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                                            else:
                                                pass 
                                        if 'place' in new_enrichment and 'geofab_lat' in geo_fab_lat and 'geofab_lon' in geo_fab_lon:
                                            new_enrichment['place']=new_enrichment['place'].astype(str)
                                            new_enrichment['place']=new_enrichment['place'].apply(lambda st: st[st.find("gnd/"):st.find("',")])
                                            new_enrichment['place']=new_enrichment['place'].str.replace("gnd/",'')
                                            new_enrichment['place_lenght']= new_enrichment['place'].str.len()
                                            new_enrichment['place']=np.where((new_enrichment['place_lenght'] == 0),'No Place', new_enrichment['place'])
                                            query_results=[]
                                            for id in new_enrichment['place']:
                                                if id == 'No Place':
                                                    birth_place_data="No Place"
                                                    name="hasGeometry"
                                                    s = pd.Series(data=birth_place_data, name=name, dtype=str)
                                                    query_results.append(s)
                                                else:
                                                    url = 'http://lobid.org/gnd/' + id + '.json'
                                                    webURL = urllib.request.urlopen(url)
                                                    lobid_data = webURL.read()
                                                    data_4 = json.loads(lobid_data.decode('utf-8'))
                                                    result_4= pd.json_normalize(data_4)
                                                    query_results.append(result_4)
                                            df=pd.concat(query_results, ignore_index=True)
                                            df=pd.DataFrame(df, columns=['hasGeometry'])
                                            df['hasGeometry'] = df['hasGeometry'].astype(str)
                                            df['lat_lon']=df['hasGeometry'].apply(lambda st: st[st.find("( +"):st.find(" )")])
                                            df['lat_lon']=df['lat_lon'].str.replace("(",'')
                                            df['lat_lon']=df['lat_lon'].str.replace("+",'')
                                            df['lat_lon']=df['lat_lon'].str.lstrip()
                                            df['lat_lon_lenght']=df['lat_lon'].str.len()
                                            df['lat_lon']=np.where((df['lat_lon_lenght'] != 0), df['lat_lon'], '0 0')
                                            df[['lon', 'lat']] = df['lat_lon'].str.split(' ', expand=True)

                                            score_table=pd.concat([geo_fab_lat['geofab_lat'], geo_fab_lon['geofab_lon'],df['lat'], df['lon']], axis=1).astype(float)
                                            score_table['geofab_lat']=score_table['geofab_lat'].fillna(method='ffill')
                                            score_table['geofab_lon']=score_table['geofab_lon'].fillna(method='ffill')
                                            score_table['distance'] = [haversine(score_table.geofab_lon[i],score_table.geofab_lat[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                        
                                            score_table['distance'] = score_table['distance'].round(decimals=1).astype(str)
                                            score_table=pd.DataFrame(score_table).astype(str)
                                            score_table['geo_distance_score_geofabrik_lobid']= np.where(((score_table['distance'] <='10.0')), 1, 0)
                                            new_enrichment=pd.concat([new_enrichment, score_table,geo_distance_score_table], axis=1)

                                        else:
                                            score_table = ['0']
                                            score_table=pd.DataFrame(score_table, columns=['geo_distance_score_geofabrik_lobid'])
                                            new_enrichment=pd.concat([new_enrichment, score_table, geo_distance_score_table], axis=1)
                                            new_enrichment['geo_distance_score_geofabrik_lobid']=new_enrichment['geo_distance_score_geofabrik_lobid'].fillna(method='ffill')
                                            score_table=new_enrichment
                                        score_table_map=score_table[score_table['geo_distance_score_geofabrik_lobid'] ==1]
                                        score_table_map = score_table_map.drop_duplicates(subset=['lon'],keep='first')
                                        for row in score_table_map.values:
                                            score=row[5]
                                            lat_org=row[2]
                                            lon_org=row[3]
                                            if score == 1:
                                                st.info("Comparison based on Geofabrik and Lobid_1")
                                                id_1=uuid.uuid4()
                                                key_id_1= str(id_1)
                                                col1, col2 = st.columns([2,2])
                                                with col1:
                                                    st.info("Source Data")
                                                    st.write(info_table)
                                                    m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                                    folium.Marker(
                                                        [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                                    ).add_to(m)
                                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                                with col2:
                                                    st.info("Target Data")
                                                    lon_lat =score_table_map
                                                    lon_lat =lon_lat.reset_index()
                                                    st.write(lon_lat)
                                                    for row in lon_lat.values:
                                                        lat=row[3]
                                                        lon=row[4]
                                                        id_2=uuid.uuid4()
                                                        key_id_2= str(id_2)
                                                        m = folium.Map(location=[lat, lon], zoom_start=16)
                                                        folium.Marker(
                                                            [lat, lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                                        ).add_to(m)
                                                        st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                                            else:
                                                pass

                                        if 'preferredName' in new_enrichment:
                                            st.info("Data from Lobid1")
                                            new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                            df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                            df_enrichment_1 =df_enrichment_1.values.tolist()

                                            name_score=[]
                                            name_scores=[]
                                            distance_score=[]
                                            distance_scores=[]
                                            for items in df_enrichment_1:
                                                filtered_list = list(filter(None, items))
                                                list_0 = filtered_info_table_list[0:]
                                                list_1 = list(set(list_0))
                                                len_list_1 = len(list_1)
                                                list_2 = list(set(filtered_list))
                                                score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                                    name_score= result1
                                                    
                                                elif 0 in score_matrix and 1 in score_matrix:
                                                    count_0=(score_matrix==0).sum()
                                                    result1 = count_0
                                                    count_1=(score_matrix==1).sum()
                                                    if count_1 == 1:
                                                        result2=0.9
                                                        name_score= result1 + result2
                                                    elif count_1 > 1:
                                                        result3=count_1*0.9
                                                        name_score= result1 + result3
                                                elif 1 in score_matrix :
                                                    count_1=(score_matrix==1).sum()
                                                    if count_1 == 1:
                                                        result2=0.9
                                                        name_score= result2
                                                    elif count_1 > 1:
                                                        result3=count_1*0.9
                                                        name_score= result3
                                                    else:
                                                        result4=0
                                                        name_score=result4                         
                                                else:
                                                    name_score = 0
                                                if name_score == len_list_1:
                                                    distance_score = 1
                                                elif name_score == (len_list_1 - 1):
                                                    distance_score = 0.75
                                                elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                                    distance_score = 0.50
                                                elif name_score < (len_list_1/2) and name_score!=0:
                                                    distance_score = 0.25
                                                else:
                                                    distance_score = 0
                                                name='distance_score_lobid'
                                                s=pd.Series(data=distance_score, name=name, dtype=float)
                                                distance_scores.append(s)

                                                name='lev_distance_lobid'
                                                s = pd.Series(data=name_score, name=name, dtype=float)
                                                name_scores.append(s)
                                            name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                            name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                            
                                            list_1 = str(', '.join(list_1))
                                            name='display_name_org'
                                            display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                            display_name_target_nds =new_enrichment['preferredName']

                                            gnd_id_score=[]
                                            gnd_id_scores=[]
                                            gnd_id_lobid=""
                                            gnd_id_lobids=[]

                                            for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                                gnd_id_lobid=gnd_id_lobid
                                                if gnd_id_org == gnd_id_lobid:
                                                    gnd_id_score= 1
                                                else:
                                                    gnd_id_score= 0
                                                s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                                gnd_id_scores.append(s)
                                                gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                                gnd_id_lobids.append(gnd_id)
                                            gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                            gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)
                                            score_table_names['gnd_id_org']=gnd_id_org
                                            score_table_names =pd.concat([score_table_names,display_name_target_nds,name_score_table_1,name_score_table_2,  gnd_id_lobids, gnd_id_scores, geo_distance_score_table, score_table['geo_distance_score_geofabrik_lobid']], axis=1, ignore_index=False) 
                                            score_table_names['lev_distance_geofabrik']=score_table_names['lev_distance_geofabrik'].fillna(method='ffill')
                                            score_table_names['distance_score_geofabrik']=score_table_names['distance_score_geofabrik'].fillna(method='ffill')
                                            score_table_names['gnd_id_org']=score_table_names['gnd_id_org'].fillna(method='ffill')
                                            score_table_names['geo_distance_score']=score_table_names['geo_distance_score'].fillna(method='ffill')
                                            score_table_names['dms_lat'] =score_table_names['dms_lat'].fillna(method='ffill')
                                            score_table_names['dms_lon'] =score_table_names['dms_lon'].fillna(method='ffill') 
                                            score_table_names['total_score'] = score_table_names['distance_score_geofabrik'].astype(float) +score_table_names['distance_score_lobid'].astype(float)+ score_table_names['gnd_id_score'].astype(float) + score_table_names['geo_distance_score'].astype(float)+score_table_names['geo_distance_score_geofabrik_lobid'].astype(float)          
                                            st.info("Total Score Table")
                                            st.write(score_table_names)
                                        
                            elif 'extratags.wikidata' not in result:
                                gnd_id=row[1]
                                gnd_id_org=str(gnd_id)
                                name=name_org
                                place=place_org

                                name=str(name)
                                for char in chars:
                                    name = name.replace(char,chars[char])
                                place=str(place)
                                for char in chars:
                                    place = place.replace(char,chars[char])

                                name_place = str(name + ' ' + place)
                                name_place = name_place.replace('&', '')
                                name_place = name_place.replace('é', 'e')
                                pattern = r'[' + string.punctuation + ']'
                                name_place = re.sub(pattern, ' ', name_place)
                                name_place= ''.join(i for i in name_place if not i.isdigit())
                                sample_converted=Convert(name_place)
                                sample_converted = [num for num in sample_converted]
                                number_of_words_1 = [len(sentence.split()) for sentence in sample_converted]
                                number_of_words_1 =sum(number_of_words_1)
                                np_lists = np.array(sample_converted)
                                output=[]
                                if number_of_words_1 == 1:
                                    output = sample_converted
                                elif number_of_words_1 > 1:
                                    filt = []
                                    for i in range(0,len(sample_converted)):
                                        filt_i = len(np_lists[i]) >= 1
                                        filt.append(filt_i)
                                    new_list = list(np_lists[filt])
                                    output = sum([list(map(list, combinations(new_list, i))) for i in range(len(new_list) + 1)], [])
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
                                    if number_of_words_1 == 1 and number_of_words ==1:
                                        path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                        col_name="search_terms"
                                        s = pd.Series(data=path1, name=col_name, dtype=str)
                                        search_results.append(s)
                                        df_results_preferred_names=pd.concat(search_results, ignore_index=True)

                                    elif number_of_words_1 > 1 and number_of_words > 1:
                                        if search_terms != name and search_terms != place:
                                            path1=str(search_terms).replace(' ','%20') + '' + '%20'
                                            col_name="search_terms"
                                            s = pd.Series(data=path1, name=col_name, dtype=str)
                                            search_results.append(s)
                                            df_results_preferred_names=pd.concat(search_results, ignore_index=True)
                                df_results_preferred_names=pd.DataFrame(df_results_preferred_names, columns=['search_terms'])
                                df_results_1 = df_results_preferred_names.drop_duplicates(subset=['search_terms'],keep='first')
                                df=[]
                                query_results=[]
                                query_id_result=[]
                                for value in df_results_1['search_terms']:
                                    address_name_search = 'https://lobid.org/gnd/search?q='+ value + '&filter=%2B%28type%3APlaceOrGeographicName%29+%2B+NOT+%28type%3ATerritorialCorporateBodyOrAdministrativeUnit%29&size=20'
                                    time.sleep(0.5)
                                    try:
                                        webURL = urllib.request.urlopen(address_name_search)
                                        data = webURL.read()
                                        data_1 = json.loads(data.decode('utf-8'))
                                        
                                        query_id= pd.json_normalize(data_1)
                                        query_id_result.append(query_id)
                                        df_id=pd.concat(query_id_result, ignore_index=True)
                            
                                        result= pd.json_normalize(data_1,'member')
                                        query_results.append(result)
                                        df=pd.concat(query_results, ignore_index=True)
                                    except urllib.error.URLError as e:
                                        code=e.code       
                                
                                columns=['id', 'totalItems']
                                df_id = df_id[columns]
                                df_id['totalItems']=df_id['totalItems'].astype(int)
                                df_id=df_id.sort_values(by='totalItems', ascending=False)
                                df_id['url'] = df_id['id'].str.replace("&format=json", "")
                                gd = GridOptionsBuilder.from_dataframe(df_id)
                                gd.configure_pagination(enabled=True)
                                gd.configure_default_column(editable=True, groupable=True)
                                gd.configure_column("url",
                                                    headerName="query_url",
                                                    cellRenderer=JsCode("""
                                                function(params) {
                                                    return '<a href=' + params.value + ' target="_blank">🖱️</a>'
                                                    }
                                                """))
                                gd.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True)
                                gridoptions = gd.build()
                                st.info("Lobid Query Sets")
                                grid_table=AgGrid(df_id, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True, key=id)
                                df = df.drop_duplicates(subset=['gndIdentifier'],keep='first')

                                df=df.reset_index()
                                columns = ['gndIdentifier','preferredName', 'geographicAreaCode','hasGeometry','placeOfBusiness', 'place', 'broaderTermInstantial', 'gndSubjectCategory']
                                columns = [col for col in columns if col in df.columns]
                                new_enrichment = df[columns]
                                score_table=[]
                                if 'place' in new_enrichment and lon_org != 'None' and lat_org != 'None':
                                    new_enrichment['place']=new_enrichment['place'].astype(str)
                                    new_enrichment['place']=new_enrichment['place'].apply(lambda st: st[st.find("gnd/"):st.find("',")])
                                    new_enrichment['place']=new_enrichment['place'].str.replace("gnd/",'')
                                    new_enrichment['place_lenght']= new_enrichment['place'].str.len()
                                    new_enrichment['place']=np.where((new_enrichment['place_lenght'] == 0),'No Place', new_enrichment['place'])
                                    query_results=[]
                                    for id in new_enrichment['place']:
                                        if id == 'No Place':
                                            birth_place_data="No Place"
                                            name="hasGeometry"
                                            s = pd.Series(data=birth_place_data, name=name, dtype=str)
                                            query_results.append(s)
                                        else:
                                            url = 'http://lobid.org/gnd/' + id + '.json'
                                            webURL = urllib.request.urlopen(url)
                                            lobid_data = webURL.read()
                                            data_4 = json.loads(lobid_data.decode('utf-8'))
                                            result_4= pd.json_normalize(data_4)
                                            query_results.append(result_4)
                                    df=pd.concat(query_results, ignore_index=True)
                                    df['hasGeometry'] = df['hasGeometry'].astype(str)
                                    df['lat_lon']=df['hasGeometry'].apply(lambda st: st[st.find("( +"):st.find(" )")])
                                    df['lat_lon']=df['lat_lon'].str.replace("(",'')
                                    df['lat_lon']=df['lat_lon'].str.replace("+",'')
                                    df['lat_lon']=df['lat_lon'].str.lstrip()
                                    df['lat_lon_lenght']=df['lat_lon'].str.len()
                                    df['lat_lon']=np.where((df['lat_lon_lenght'] != 0), df['lat_lon'], '0 0')
                                    df[['lon', 'lat']] = df['lat_lon'].str.split(' ', expand=True)

                                    score_table=pd.concat([geo_fab_lat['geofab_lat'], geo_fab_lon['geofab_lon'],df['lat'], df['lon']], axis=1).astype(float)
                                    score_table['geofab_lat']=score_table['geofab_lat'].fillna(method='ffill')
                                    score_table['geofab_lon']=score_table['geofab_lon'].fillna(method='ffill')
                                    score_table['distance'] = [haversine(score_table.geofab_lon[i],score_table.geofab_lat[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                
                                    score_table['distance'] = score_table['distance'].round(decimals=1).astype(str)
                                    score_table=pd.DataFrame(score_table).astype(str)
                                    score_table['geo_distance_score_lobid']= np.where(((score_table['distance'] <='10.0')), 1, 0)
                                    new_enrichment=pd.concat([new_enrichment, score_table,geo_distance_score_table], axis=1)
                                    
                                else:
                                    score_table = ['0']
                                    score_table=pd.DataFrame(score_table, columns=['geo_distance_score_lobid'])
                                    new_enrichment=pd.concat([new_enrichment, score_table], axis=1)
                                    new_enrichment['geo_distance_score_lobid']=new_enrichment['geo_distance_score_lobid'].fillna(method='ffill')
                                    
                                    score_table=new_enrichment

                                for score in score_table['geo_distance_score_lobid']:

                                    if score == 1 and lon_org != 'None' and lat_org != 'None':
                                        st.info("Comparison based on Lobid")
                                        id_1=uuid.uuid4()
                                        key_id_1= str(id_1)
                                        col1, col2 = st.columns([2,2])
                                        with col1:
                                            st.info("Source Data")
                                            st.write(info_table)
                                            m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                            folium.Marker(
                                                [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                            ).add_to(m)
                                            st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                        with col2:
                                            st.info("Target Data")
                                            lon_lat =new_enrichment[new_enrichment['geo_distance_score_lobid'] ==1]
                                            lon_lat =lon_lat.reset_index()
                                            st.write(lon_lat)
                                            for lat in lon_lat['lat']:
                                                for lon in lon_lat['lon']:
                                                    lon = lon
                                                    lat=lat
                                                    id_2=uuid.uuid4()
                                                    key_id_2= str(id_2)
                                                m = folium.Map(location=[lat, lon], zoom_start=16)
                                                folium.Marker(
                                                    [lat, lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                                ).add_to(m)
                                                st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                                    else:
                                        pass

                                if 'place' in new_enrichment and 'lat' in geo_fab_lat and 'lon' in geo_fab_lon:
                                    st.write("geo_fab_lat:", geo_fab_lat)
                                    st.write("geo_fab_lon:", geo_fab_lon)
                                    new_enrichment['place']=new_enrichment['place'].astype(str)
                                    new_enrichment['place']=new_enrichment['place'].apply(lambda st: st[st.find("gnd/"):st.find("',")])
                                    new_enrichment['place']=new_enrichment['place'].str.replace("gnd/",'')
                                    new_enrichment['place_lenght']= new_enrichment['place'].str.len()
                                    new_enrichment['place']=np.where((new_enrichment['place_lenght'] == 0),'No Place', new_enrichment['place'])
                                    query_results=[]
                                    for id in new_enrichment['place']:
                                        if id == 'No Place':
                                            birth_place_data="No Place"
                                            name="hasGeometry"
                                            s = pd.Series(data=birth_place_data, name=name, dtype=str)
                                            query_results.append(s)
                                        else:
                                            url = 'http://lobid.org/gnd/' + id + '.json'
                                            webURL = urllib.request.urlopen(url)
                                            lobid_data = webURL.read()
                                            data_4 = json.loads(lobid_data.decode('utf-8'))
                                            result_4= pd.json_normalize(data_4)
                                            query_results.append(result_4)
                                    df=pd.concat(query_results, ignore_index=True)
                                    df['hasGeometry'] = df['hasGeometry'].astype(str)
                                    df['lat_lon']=df['hasGeometry'].apply(lambda st: st[st.find("( +"):st.find(" )")])
                                    df['lat_lon']=df['lat_lon'].str.replace("(",'')
                                    df['lat_lon']=df['lat_lon'].str.replace("+",'')
                                    df['lat_lon']=df['lat_lon'].str.lstrip()
                                    df['lat_lon_lenght']=df['lat_lon'].str.len()
                                    df['lat_lon']=np.where((df['lat_lon_lenght'] != 0), df['lat_lon'], '0 0')
                                    df[['lon', 'lat']] = df['lat_lon'].str.split(' ', expand=True)

                                    score_table=pd.concat([geo_fab_lat['geofab_lat'], geo_fab_lon['geofab_lon'],df['lat'], df['lon']], axis=1).astype(float)
                                    score_table['geofab_lat']=score_table['geofab_lat'].fillna(method='ffill')
                                    score_table['geofab_lon']=score_table['geofab_lon'].fillna(method='ffill')
                                    score_table['distance'] = [haversine(score_table.geofab_lon[i],score_table.geofab_lat[i],score_table.lon[i],score_table.lat[i]) for i in range(len(score_table))]
                                
                                    score_table['distance'] = score_table['distance'].round(decimals=1).astype(str)
                                    score_table=pd.DataFrame(score_table).astype(str)
                                    score_table['geo_distance_score_geofabrik_lobid']= np.where(((score_table['distance'] <='10.0')), 1, 0)
                                    new_enrichment=pd.concat([new_enrichment, score_table,geo_distance_score_table], axis=1)
                                else:
                                    score_table = ['0']
                                    score_table=pd.DataFrame(score_table, columns=['geo_distance_score_geofabrik_lobid'])
                                    new_enrichment=pd.concat([new_enrichment, score_table, geo_distance_score_table], axis=1)
                                    new_enrichment['geo_distance_score_geofabrik_lobid']=new_enrichment['geo_distance_score_geofabrik_lobid'].fillna(method='ffill')
                                    score_table=new_enrichment
                                for score in score_table['geo_distance_score_geofabrik_lobid']:

                                    if score == 1 and lon_org != 'None' and lat_org != 'None':
                                        st.info("Comparison based on Geofabrik and Lobid")
                                        id_1=uuid.uuid4()
                                        key_id_1= str(id_1)
                                        col1, col2 = st.columns([2,2])
                                        with col1:
                                            st.info("Source Data")
                                            st.write(info_table)
                                            m = folium.Map(location=[lat_org, lon_org], zoom_start=16)
                                            folium.Marker(
                                                [lat_org, lon_org], popup="Liberty Bell", tooltip="Liberty Bell"
                                            ).add_to(m)
                                            st_data = st_folium(m, width=725, returned_objects=[], key=key_id_1)
                                        with col2:
                                            st.info("Target Data")
                                            lon_lat =new_enrichment[new_enrichment['geo_distance_score_geofabrik_lobid'] ==1]
                                            lon_lat =lon_lat.reset_index()
                                            st.write(lon_lat)
                                            for lat in lon_lat['lat']:
                                                for lon in lon_lat['lon']:
                                                    lon = lon
                                                    lat=lat
                                                    id_2=uuid.uuid4()
                                                    key_id_2= str(id_2)
                                                m = folium.Map(location=[lat, lon], zoom_start=16)
                                                folium.Marker(
                                                    [lat, lon], popup="Liberty Bell", tooltip="Liberty Bell"
                                                ).add_to(m)
                                                st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                                    else:
                                        pass

                                st.info("Data from Lobid")
                                if 'preferredName' in new_enrichment:
                                    info_table['display_name_org']=info_table['display_name_org'].str.replace(r'[^\w\s]+', '')
                                    info_table_1=info_table['display_name_org'].str.split(expand=True)
                                    info_table_1=info_table_1.values.tolist()
                                    filtered_info_table_list=[x for x in info_table_1[0] if "None" not in x]                     
                                    new_enrichment['preferredName']=new_enrichment['preferredName'].str.replace(r'[^\w\s]+', '')
                                    df_enrichment_1=new_enrichment['preferredName'].str.split(expand=True)
                                    df_enrichment_1 =df_enrichment_1.values.tolist()

                                    name_score=[]
                                    name_scores=[]
                                    distance_score=[] 
                                    distance_scores=[]
                                    for items in df_enrichment_1:
                                        filtered_list = list(filter(None, items))
                                        list_0 = filtered_info_table_list[0:]
                                        list_1 = list(set(list_0))
                                        
                                        len_list_1 = len(list_1)
                                        list_2 = list(set(filtered_list))
                                        score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                            name_score= result1
                                            
                                        elif 0 in score_matrix and 1 in score_matrix:
                                            count_0=(score_matrix==0).sum()
                                            result1 = count_0
                                            count_1=(score_matrix==1).sum()
                                            if count_1 == 1:
                                                result2=0.9
                                                name_score= result1 + result2
                                            elif count_1 > 1:
                                                result3=count_1*0.9
                                                name_score= result1 + result3
                                        elif 1 in score_matrix :
                                            count_1=(score_matrix==1).sum()
                                            if count_1 == 1:
                                                result2=0.9
                                                name_score= result2
                                            elif count_1 > 1:
                                                result3=count_1*0.9
                                                name_score= result3
                                            else:
                                                result4=0
                                                name_score=result4                         
                                        else:
                                            name_score = 0
                                        if name_score == len_list_1:
                                            distance_score = 1
                                        elif name_score == (len_list_1 - 1):
                                            distance_score = 0.75
                                        elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                            distance_score = 0.50
                                        elif name_score < (len_list_1/2) and name_score!=0:
                                            distance_score = 0.25
                                        else:
                                            distance_score = 0
                                        name='distance_score_lobid'
                                        s=pd.Series(data=distance_score, name=name, dtype=float)
                                        distance_scores.append(s)

                                        name='lev_distance_lobid'
                                        s = pd.Series(data=name_score, name=name, dtype=float)
                                        name_scores.append(s)
                                    name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                                    name_score_table_2 = pd.concat(distance_scores, ignore_index=True)
                                    
                                    list_1 = str(', '.join(list_1))
                                    name='display_name_org'
                                    display_name_org = pd.Series(data=list_1, name=name, dtype=str)
                                    display_name_target =new_enrichment['preferredName']
                                    score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1,name_score_table_2],axis=1, ignore_index=False)
                                    score_table_names=pd.DataFrame(score_table_names)
                                    gnd_id_score=[]
                                    gnd_id_scores=[]
                                    gnd_id_lobid=""
                                    gnd_id_lobids=[]
                                    for gnd_id_lobid in new_enrichment['gndIdentifier'].values:
                                        gnd_id_lobid=gnd_id_lobid
                                        if gnd_id_org == gnd_id_lobid:
                                            gnd_id_score= 1
                                        else:
                                            gnd_id_score= 0
                                        s= pd.Series(data=gnd_id_score, name='gnd_id_score', dtype=str)
                                        gnd_id_scores.append(s)
                                        gnd_id=pd.Series(data=gnd_id_lobid, name='gnd_id_lobid', dtype=str)
                                        gnd_id_lobids.append(gnd_id)
                                    gnd_id_scores = pd.concat(gnd_id_scores, ignore_index=True)
                                    gnd_id_lobids=pd.concat(gnd_id_lobids,ignore_index=True)

                                    if 'display_name_org' in score_table_names:
                                        score_tables= pd.concat([score_table_names, gnd_id_lobids, gnd_id_scores, geo_distance_score_table, score_table['geo_distance_score_lobid'], score_table['geo_distance_score_geofabrik_lobid']], axis=1)
                                        score_tables=pd.DataFrame(score_tables)
                                        score_tables['dms_lat'] =score_tables['dms_lat'].fillna(method='ffill')
                                        score_tables['dms_lon'] =score_tables['dms_lon'].fillna(method='ffill')
                                        score_tables['total_score'] = score_tables['distance_score_lobid'].astype(float)+score_tables['gnd_id_score'].astype(float)+score_tables['geo_distance_score_lobid'].astype(float)+score_tables['geo_distance_score_geofabrik_lobid'].astype(float)
                                        st.info("Total Score Table")
                                        st.write(score_tables)
                                    else:
                                        score_table=pd.concat([display_name_org, new_enrichment], axis=1)
                                        score_table['total_score'] = score_table['geo_distance_score_lobid'].astype(float)+score_table['geo_distance_score_geofabrik_lobid'].astype(float)
                                        st.info("Total Score Table")
                                        st.write(score_table.astype(str))               
                if save_status == 'Speichern':
                    fn_bar()
                    st.success('Ihre Ergebnisse wurden erfolgreich gespeichert.', icon="✅")
                    def tac():
                        t_sec = round(time.time() - start_time)
                        (t_min, t_sec) = divmod(t_sec,60)
                        (t_hour,t_min) = divmod(t_min,60)
                        st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                    tac()

                elif save_status == 'Nicht speichern':
                    def tac():
                        t_sec = round(time.time() - start_time)
                        (t_min, t_sec) = divmod(t_sec,60)
                        (t_hour,t_min) = divmod(t_min,60)
                        st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                    tac()

def fn_buildings_private_data_wikidata(data):
    username=session_state.username
    df = data
    grouped = df.groupby(['alternative_title'])
    group_lst=df['alternative_title'].drop_duplicates(keep='first').tolist()
    objects = st.container()
    object_list = df['alternative_title'].unique()
    object_list =pd.DataFrame(object_list)
    free_df="<Wählen>"
    free_df=[free_df]
    free_df = pd.DataFrame(free_df)
    object_list=pd.concat([free_df,object_list],axis=0)
    
    choice = objects.selectbox("Choose a Dataset", object_list)
    if choice=="<Wählen>":
        st.write("Bitte wählen Sie einen zu verarbeitenden Datensatz aus.")
    else:
        st.write("ausgewählte private Daten:", choice)
        rows_count = df.count()[0]
        rows_count == df[df.columns[0]].count()

        st.warning("Bitte wählen Sie, ob Sie die Ergebnisse speichern möchten.")
        save_status = st.radio("Ergebnisse speichern.", ('Speichern', 'Nicht speichern'), key='2')
        now = datetime.datetime.now()
        type_entity ="Bauwerke"
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

        limit_type = st.radio("Limit", ('Standard', 'Random'), key='buildings')
        df = grouped.get_group(choice)
        df['index'] =df['dc_identifier'].astype(str)
        df =df.sort_values(by='index', ascending=True)
        df =df.drop_duplicates(subset=['dc_identifier'],keep='first')
        df_indexed = df.reset_index(drop=True, inplace=True)
        rows_count = df.count()[0]
        rows_count == df[df.columns[0]].count()
        
        st.write("Die Gesamtanzahl der Zeilen in diesem Datensatz beträgt:", rows_count)
        limit_slider_standard=""
        limit_slider_random =""
        if limit_type == 'Standard':
            limit = limit_type
            slider_range=st.slider("double ended slider", value=[0, (int(rows_count))])
            st.write("slider range:", slider_range, slider_range[0], slider_range[1])
            slider_range_1=slider_range[0]
            slider_range_2=slider_range[1]
            candidates=df.iloc[slider_range_1:slider_range_2]
            selected_rows_count = candidates[candidates.columns[0]].count()
            limit_slider_standard =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))
        
        elif limit_type == 'Random':
            limit = limit_type
            range_start = st.number_input('Range Start', value=0, key='random_1')
            range_end = st.number_input('Range End', value=(int(rows_count)), key='random_2')
            slider_range=st.slider("double ended slider", value=[range_start, range_end])
            st.write("slider range:", slider_range, slider_range[0], slider_range[1])
            slider_range_1=slider_range[0]
            slider_range_2=slider_range[1]
            candidates=df.iloc[slider_range_1:slider_range_2]
            selected_rows_count = candidates[candidates.columns[0]].count()
            limit_slider_random =st.slider('Bitte wählen Sie Ihre Limit', 1, (int(selected_rows_count)))

        if limit_slider_standard:
            query_candidates=candidates.iloc[0:limit_slider_standard]
            st.write(query_candidates)
        elif limit_slider_random:
            query_candidates = candidates.sample(n=limit_slider_random, replace=True, random_state=1)
            query_candidates = st.data_editor(query_candidates)
            query_candidates =query_candidates.astype(str)
            query_candidates =query_candidates.drop_duplicates(subset=['dc_identifier'],keep='first')

        selected = st.radio("Bitte Wählen", ('nach alternate Name filtern', 'nach alternate Name filtern (by search)', 'nach alternate Platz filtern', 'nach Objekt Id filtern', 'nach Date of Production filtern'))
        if selected == 'nach alternate Name filtern':
            query_candidates =query_candidates.astype(str)       

        elif selected == 'nach alternate Name filtern (by search)':
            query_candidates =query_candidates.astype(str)
            options =query_candidates['alternate_name'].unique()
            selected_options =st.multiselect("Name Wählen", options)
            query_candidates = query_candidates.loc[query_candidates["alternate_name"].isin(selected_options)]

        elif selected == 'nach alternate Platz filtern':
            query_candidates =query_candidates.astype(str)
            options =query_candidates['abbreviated_name'].unique()
            selected_options =st.multiselect("Name Wählen", options)
            query_candidates = query_candidates.loc[query_candidates['abbreviated_name'].isin(selected_options)]

        elif selected == 'nach Objekt Id filtern':                    
            query_candidates =query_candidates.astype(str)
            options =query_candidates['dc_identifier'].unique()
            selected_options =st.multiselect("Objekt Id Wählen", options)
            query_candidates = query_candidates.loc[query_candidates["dc_identifier"].isin(selected_options)]
        elif selected == 'nach Date of Production filtern':
            st.write("Thisfilter is under construction.")
        else:
            pass
                    
        if st.button("Load Data", key='load_1'):
            st.info("Bitte warten Sie, bis der Abfrage-, Zuordnungs- und Bewertungsprozess beendet ist, und verlassen Sie die Seite nicht.")
            start_time = time.time()
            def tic():
                global start_time 
                start_time = time.time()
            for row in query_candidates.values:
                

                name_org = row[7]
                dc_identifier=row[1]
                name_org_org = row[7]

                name_org= ''.join(i for i in name_org if not i.isdigit())
                pattern = r'[' + string.punctuation + ']'

                name_org = re.sub(pattern, ' ', name_org)
                name_org=name_org.replace(',', '')
                name_org=name_org.replace('.', '')
                name_org=name_org.replace('"', '')
                name_org=name_org.replace('-', ' ')
                name_org=name_org.replace('St.', '')
                name_org=name_org.replace(' der ', ' ')
                name_org=name_org.replace(' des ', ' ')
                name_org=name_org.replace(' die ', ' ')
                name_org=name_org.replace(' für ', ' ')
                name_org=name_org.replace(' in ', ' ')
                name_org=name_org.replace(' & ', ' ')
                name_org=name_org.replace(' und ', ' ')
                name_org=name_org.replace(' am ', ' ')
                words = name_org.split()
                if len(words) >=5:
                    words=words[0:4]
                else:
                    pass
                search_terms = []

                street_address_org=row[18]
                place_org=row[4]
                lat_org=row[22]
                lon_org=row[23]
                var_geo_name_org=row[9]
                geo_name_literal_org =row[16]
                

                info_table = [{'dc_identifier':dc_identifier, 'name':name_org, 'street_address':street_address_org,'place':place_org,'lat_org':lat_org,'lon_org':lon_org, 'display_name_org': name_org+', '+street_address_org+', '+place_org+', '+var_geo_name_org+', '+geo_name_literal_org}]
                info_table=pd.DataFrame(info_table)
               
                # Generate combinations of words
                for r in range(2, len(words) + 1):
                    for combo in combinations(words, r):
                        search_terms.append(" ".join(combo))
                mylist = search_terms
                mystring = '"' + '" "'.join(mylist) + '"'
                endpoint_url = "https://query.wikidata.org/sparql"
                    
                query = """SELECT ?item ?label ?countryLabel ?description ?coordinates ?gndId
                    WHERE {
                    VALUES ?term { %s }
                    SERVICE wikibase:mwapi {
                        bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                        wikibase:api "EntitySearch";
                                        mwapi:search ?term;
                                        mwapi:language "de".
                        ?item wikibase:apiOutputItem mwapi:item.
                        ?num wikibase:apiOrdinal true.
                        ?label wikibase:apiOutput mwapi:label.
                    }
                    ?item wdt:P17 wd:Q183.
                    OPTIONAL {?item wdt:P17 ?country}.
                    OPTIONAL {?item wdt:P625 ?coordinates}.
                    OPTIONAL {?coordinates rdf:value ?coordinatesValue}.
                    OPTIONAL {?item schema:description ?description}.
                    OPTIONAL {?item wdt:P227 ?gndId}.  # Get the GND ID
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],de" }
                    } ORDER BY ASC(?num) LIMIT 100""" % mystring


                def get_results(endpoint_url, query):
                    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
                    # TODO adjust user agent; see https://w.wiki/CX6
                    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
                    sparql.setQuery(query)
                    sparql.setReturnFormat(JSON)
                    return sparql.query().convert()
                time.sleep(0.5)
                results = get_results(endpoint_url, query)
                results_df = pd.json_normalize(results['results']['bindings'])
                if 'description.xml:lang' in results_df:
                    results_df=results_df.loc[results_df['description.xml:lang'] =="de"]
                else:
                    pass
                results_df =pd.DataFrame(results_df, columns=['label.value','item.value','countryLabel.xml:lang','description.value','coordinates.value'])
                results_df=results_df.reset_index()
                results_df['wikidata_id']=results_df['item.value']
                results_df['display_name_target_wikidata_org']=results_df['label.value']
                results_df['wikidata_coordinates']=results_df['coordinates.value']
                results_df['coordinates']=results_df['coordinates.value'] 
                if save_status == 'Speichern':
                    results_df=results_df.astype(str) 
                    name_org=name_org.strip()
                    name_org=name_org.lower()
                    name_org_list = name_org.split(' ')
                    if 'label.value' in results_df and results_df['label.value'].count()!=0:
                        results_df = results_df.drop_duplicates(subset=['item.value'],keep='first')
                        results_df['label.value'] = results_df['label.value'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                        results_df['label.value'] = results_df['label.value'].replace(r' ', ' ', regex=True)
                        results_df['label.value']=results_df['label.value'].str.replace(r'[^\w\s]+', ' ')
                        results_df['label.value']=results_df['label.value'].str.lower()
                        results_df_1=results_df['label.value'].str.split(expand=True)
                        results_df_1=results_df_1.values.tolist()
                        name_score=[]
                        name_scores=[]
                        distance_score=[] 
                        distance_scores=[]
                        for items in results_df_1:
                            filtered_list = list(filter(None, items))
                            list_0 = name_org_list
                            list_1 = list(set(list_0))
                            
                            len_list_1 = len(list_1)
                            list_2 = list(set(filtered_list))
                            score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                name_score= result1
                                
                            elif 0 in score_matrix and 1 in score_matrix:
                                count_0=(score_matrix==0).sum()
                                result1 = count_0
                                count_1=(score_matrix==1).sum()
                                if count_1 == 1:
                                    result2=0.9
                                    name_score= result1 + result2
                                elif count_1 > 1:
                                    result3=count_1*0.9
                                    name_score= result1 + result3
                            elif 1 in score_matrix :
                                count_1=(score_matrix==1).sum()
                                if count_1 == 1:
                                    result2=0.9
                                    name_score= result2
                                elif count_1 > 1:
                                    result3=count_1*0.9
                                    name_score= result3
                                else:
                                    result4=0
                                    name_score=result4                         
                            else:
                                name_score = 0
                            if name_score == len_list_1:
                                distance_score = 1
                            elif name_score == (len_list_1 - 1):
                                distance_score = 0.75
                            elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                distance_score = 0.50
                            elif name_score < (len_list_1/2) and name_score!=0:
                                distance_score = 0.25
                            else:
                                distance_score = 0
                            name='lev_distance_score_wikidata'
                            s=pd.Series(data=distance_score, name=name, dtype=float)
                            distance_scores.append(s)
                            name='lev_distance_wikidata'
                            s = pd.Series(data=name_score, name=name, dtype=float)
                            name_scores.append(s)
                        name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                        name_score_table_2 = pd.concat(distance_scores, ignore_index=True)

                        results_df=results_df.drop(results_df[(results_df['coordinates'] =='nan')].index)
                        dms_table=[]
                        if 'coordinates' in results_df and results_df['coordinates'].count()!=0:
                            results_df['coordinates']=results_df['coordinates'].str.replace('Point', '')
                            results_df['coordinates']=results_df['coordinates'].str.replace('(', '')
                            results_df['coordinates']=results_df['coordinates'].str.replace(')', '')
                            results_df[['lon', 'lat']] = results_df['coordinates'].str.split(' ', 1, expand=True)
                            results_df=results_df.astype(str)
                            results_df_list=results_df[['lon', 'lat']].values.tolist()
                            dms_lats =[]
                            dms_lons =[]
                            for row in results_df_list:
                                lon=row[0]
                                lat= row[1]
                                id_2=uuid.uuid4()
                                key_id_2= str(id_2)            
                                dms=dd2dms(float(lat), float(lon))
                                dms_lat=dms[0]
                                name='dms_lat'
                                s=pd.Series(data=dms_lat, name=name, dtype=str)
                                dms_lats.append(s)
                                dms_lon=dms[1]
                                name='dms_lon'
                                s=pd.Series(data=dms_lon, name=name, dtype=str)
                                dms_lons.append(s)
                            dms_lat_table = pd.concat(dms_lats, ignore_index=True)
                            dms_lon_table = pd.concat(dms_lons, ignore_index=True)
                        dms_table = pd.concat([dms_lat_table, dms_lon_table],axis=1, ignore_index=False)
                        name='display_name_org'
                        display_name_org = pd.Series(data=name_org, name=name, dtype=str)

                        results_df_2=results_df['label.value'].reset_index()
                        results_df_2=results_df_2.rename(columns={'label.value':'display_name_target_wikidata'})
                        display_name_target =results_df_2
                        score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1, name_score_table_2,results_df['display_name_target_wikidata_org'],results_df['wikidata_id'],results_df['wikidata_coordinates'], dms_table],axis=1, ignore_index=False)
                        score_table_names=pd.DataFrame(score_table_names)
                        score_table_names['total_score']=name_score_table_2
                        info_table=pd.concat([info_table, meta_data], axis=1)
                        #info_table=pd.DataFrame(info_table).rename(columns={"id":"dc_identifier"})
                        info_table['target_query_system'] =str('wikidata')
                        st.write(info_table)
                        conn=db_connect()
                        info_table.to_sql(name='queried_private_building_data', con=conn, if_exists='append', chunksize = 5000, method='multi') 
                        all_scores_to_save=pd.concat([meta_data,score_table_names], axis=1)
                        all_scores_to_save['date_of_query'] =all_scores_to_save['date_of_query'].fillna(method='ffill')
                        all_scores_to_save['default_title'] =all_scores_to_save['default_title'].fillna(method='ffill')
                        all_scores_to_save['alternative_title'] =all_scores_to_save['alternative_title'].fillna(method='ffill')
                        all_scores_to_save['comment_content'] =all_scores_to_save['comment_content'].fillna(method='ffill')
                        all_scores_to_save['display_name_org'] =all_scores_to_save['display_name_org'].fillna(method='ffill')
                        all_scores_to_save['source_data_id']=info_table['dc_identifier']
                        all_scores_to_save['source_data_id']=all_scores_to_save['source_data_id'].fillna(method='ffill')
                        all_scores_to_save['queried_by']=all_scores_to_save['queried_by'].fillna(method='ffill')
                        all_scores_to_save['public']=all_scores_to_save['public'].fillna(method='ffill')
                        all_scores_to_save['target_query_system'] =str('wikidata')
                        match_status =" "
                        comment = " "
                        all_scores_to_save['match_status']=match_status
                        all_scores_to_save['comment']=comment                        
                        all_scores_to_save = all_scores_to_save.applymap(str)
                        all_scores_to_save.to_sql(name='private_building_data_results', con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')

                elif save_status == 'Nicht speichern':
                    with st.expander(dc_identifier + ' ' +name_org_org):
                        st.info("Wikdata Query Results.")
                        st.write(results_df)
                        results_df=results_df.astype(str)
                        name_org=name_org.strip()
                        name_org=name_org.lower()
                        name_org_list = name_org.split(' ')
                        if 'label.value' in results_df and results_df['label.value'].count()!=0:
                            results_df = results_df.drop_duplicates(subset=['item.value'],keep='first')
                            results_df['label.value'] = results_df['label.value'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                            results_df['label.value'] = results_df['label.value'].replace(r' ', ' ', regex=True)
                            results_df['label.value']=results_df['label.value'].str.replace(r'[^\w\s]+', ' ')
                            results_df['label.value']=results_df['label.value'].str.lower()
                            results_df_1=results_df['label.value'].str.split(expand=True)
                            results_df_1=results_df_1.values.tolist()
                            name_score=[]
                            name_scores=[]
                            distance_score=[] 
                            distance_scores=[]
                            for items in results_df_1:
                                filtered_list = list(filter(None, items))
                                list_0 = name_org_list
                                list_1 = list(set(list_0))
                                
                                len_list_1 = len(list_1)
                                list_2 = list(set(filtered_list))
                                score_matrix = np.zeros((len(list_1),len(list_2)),dtype=np.int32)
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
                                    name_score= result1
                                    
                                elif 0 in score_matrix and 1 in score_matrix:
                                    count_0=(score_matrix==0).sum()
                                    result1 = count_0
                                    count_1=(score_matrix==1).sum()
                                    if count_1 == 1:
                                        result2=0.9
                                        name_score= result1 + result2
                                    elif count_1 > 1:
                                        result3=count_1*0.9
                                        name_score= result1 + result3
                                elif 1 in score_matrix :
                                    count_1=(score_matrix==1).sum()
                                    if count_1 == 1:
                                        result2=0.9
                                        name_score= result2
                                    elif count_1 > 1:
                                        result3=count_1*0.9
                                        name_score= result3
                                    else:
                                        result4=0
                                        name_score=result4                         
                                else:
                                    name_score = 0
                                if name_score == len_list_1:
                                    distance_score = 1
                                elif name_score == (len_list_1 - 1):
                                    distance_score = 0.75
                                elif name_score==(len_list_1/2) or (name_score - 0.5) == (len_list_1/2)-1 or (name_score + 0.5) == (len_list_1/2)+1:
                                    distance_score = 0.50
                                elif name_score < (len_list_1/2) and name_score!=0:
                                    distance_score = 0.25
                                else:
                                    distance_score = 0
                                name='distance_score'
                                s=pd.Series(data=distance_score, name=name, dtype=float)
                                distance_scores.append(s)
                                name='lev_distance'
                                s = pd.Series(data=name_score, name=name, dtype=float)
                                name_scores.append(s)
                            name_score_table_1 = pd.concat(name_scores, ignore_index=True)
                            name_score_table_2 = pd.concat(distance_scores, ignore_index=True)

                            name='display_name_org'
                            display_name_org = pd.Series(data=name_org, name=name, dtype=str)

                            results_df_2=results_df['label.value'].reset_index()
                            display_name_target =results_df_2
                            score_table_names=pd.concat([display_name_org,display_name_target,name_score_table_1,name_score_table_2],axis=1, ignore_index=False)
                            score_table_names=pd.DataFrame(score_table_names)
                            st.info("Score Table:")
                            st.write(score_table_names)                                
                        results_df=results_df.drop(results_df[(results_df['coordinates.value'] =='nan')].index)
                        if 'coordinates.value' in results_df and results_df['coordinates.value'].count()!=0:
                            results_df['coordinates.value']=results_df['coordinates.value'].str.replace('Point', '')
                            results_df['coordinates.value']=results_df['coordinates.value'].str.replace('(', '')
                            results_df['coordinates.value']=results_df['coordinates.value'].str.replace(')', '')
                            results_df[['lon', 'lat']] = results_df['coordinates.value'].str.split(' ', 1, expand=True)
                            results_df=results_df.astype(str)
                            results_df_list=results_df[['description.value','lon', 'lat', 'display_name_target_wikidata_org','wikidata_id']].values.tolist()
                            for row in results_df_list:
                                description_value= row[0]
                                lon=row[1]
                                lat= row[2]
                                display_name_target_wikidata_org=row[3]
                                wikidata_id=row[4]
                                st.info(wikidata_id)
                                list_of_values=[display_name_target_wikidata_org,description_value,wikidata_id, lon, lat]
                                id_2=uuid.uuid4()
                                key_id_2= str(id_2)            
                                dms=dd2dms(float(lat), float(lon))
                                dms=pd.DataFrame(dms)
                                m = folium.Map(location=[lat, lon], zoom_start=16)
                                folium.Marker(
                                    [lat, lon], popup=display_name_target_wikidata_org, tooltip=display_name_target_wikidata_org
                                ).add_to(m)
                                col1, col2 = st.columns([2,2])
                                with col1:
                                    df = pd.DataFrame(list_of_values)
                                    df =pd.concat([df, dms], axis=0, ignore_index=True)
                                    st.write(df)
                                with col2:
                                    st_data = st_folium(m, width=725, returned_objects=[], key=key_id_2)
                        else:
                            pass
            if save_status == 'Speichern':
                fn_bar()
                st.success('Ihre Ergebnisse wurden erfolgreich gespeichert.', icon="✅")
                def tac():
                    t_sec = round(time.time() - start_time)
                    (t_min, t_sec) = divmod(t_sec,60)
                    (t_hour,t_min) = divmod(t_min,60)
                    st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                tac()

            elif save_status == 'Nicht speichern':
                def tac():
                    t_sec = round(time.time() - start_time)
                    (t_min, t_sec) = divmod(t_sec,60)
                    (t_hour,t_min) = divmod(t_min,60)
                    st.success('Die Aufgabe wurde in dieser Zeit erledigt: {} hour: {} min: {} sec'.format(t_hour,t_min,t_sec))
                tac()
                                        