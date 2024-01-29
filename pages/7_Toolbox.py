import streamlit as st
import pandas as pd
from streamlit_option_menu import option_menu
from data.private_data_matching import fn_private_data, fun_wikidata_matching_scoring, fn_buildings_private_data_lobid, fn_buildings_private_data_wikidata
from data.data_import import fn_file_download, person_data_upload, building_data_upload, fn_csv_file_download
from data.results import private_data_results, building_data_result
from data.reporting import fun_reporting
from data.data_visualize import fun_visualize_person_data, fun_visualize_building_data
from data.data_profiling import fun_data_profiling_person,fun_data_profiling_building
from blog.blogpost import add_blog_post
from users.user_functions import list_users, update_user, delete_user,user_registration,get_user_by_username,update_single_user
from data.recon_api_matching import fn_private_data_reconapi
import requests
import datetime
from datetime import datetime
from time import sleep
import uuid
import json
from sqlalchemy import create_engine
from data.toolbox_documentation import list_data, database_diagram,download_document,download_import_document,download_installation_document
from users.session_state import session_state
from db.db_functions import db_connect
import pathlib

st.set_page_config(page_title='Toolbox', layout="wide")


def is_logged_in():
    return hasattr(st.session_state, 'logged_in') and st.session_state.logged_in

login_username = st.sidebar.text_input("Username", key="login_username")
login_password = st.sidebar.text_input("Password", type="password", key="login_password")


if st.sidebar.button("Login", key="login"):
    login_response = requests.post(
        "http://localhost:8000/login", verify=False,
        data={"username": login_username, "password": login_password}
    )
    if login_response.status_code == 200:
        st.sidebar.success("Login successful")
        st.session_state.logged_in = True  # Set session state variable for login
        session_state.username = login_username
        
    else:
        st.error("Invalid credentials")

# Logout Button
if hasattr(st.session_state, 'logged_in') and st.session_state.logged_in:
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.sidebar.info("Logged out successfully")

API_BASE_URL = "http://127.0.0.1:8000"
def get_person_private_data():
    response = requests.get(f"{API_BASE_URL}/get_person_private_data")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")

def get_building_private_data():
    response = requests.get(f"{API_BASE_URL}/get_building_private_data")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")

def get_panel_data():
    response = requests.get(f"{API_BASE_URL}/get_panel_data")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")

if is_logged_in():
    username=login_username
    
    user_name = username
    # Fetch existing user data if user ID is provided
    user_id = None
    user_email= None
    if user_name:
        BACKEND_URL = "http://localhost:8000"
        response = requests.get(f"{BACKEND_URL}/users/{username}")
        if response.status_code == 200:
           user_data = response.json()
           user_id=user_data[0]['id']
           user_role=user_data[0]['userrole']

    with st.sidebar:
        col1, col2  = st.columns([3,1])
        with col1:
            select_option = st.radio("Features Sub-Menu", options=['Toolbox', 'Matching', 'Data Import','Ergebnisse', 'Reporting', 'Data Profiling','Data Visualisierung', 'Blogpost', 'Benutzer Verwaltung'], horizontal=False, label_visibility="visible")

    if select_option=='Toolbox':
        st.header("Toolbox")
        st.write('In diesem Teil unserer Toolbox finden Sie eine ausführliche Dokumentation zum privaten Datenimport, zum Benutzerfluss in der Toolbox-Umgebung und zu Datenbankdiagrammen.')
        with st.expander("How the Toolbox works"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("Document for App Installation")
                download_installation_document()
            with col2:
                st.write("Document for Data Processing")
                download_document()
            with col3:
                st.write("Document for Data Import")
                download_import_document()
        with st.expander("User-flow in Toolbox"):
            list_data()

    elif select_option=="Matching":
        st.header("Matching")
        tab1, tab2 = st.tabs(["Matching für Personen", "Matching für Bauwerke"])	

        with tab1:
            target_system = st.radio("Wählen Sie bitte eine Zieldatenquelle",('Lobid (Standard)', 'Lobid (Reconciliation API)', 'Wikidata'), horizontal=True, label_visibility="visible")
            if target_system =='Lobid (Standard)' or target_system =='Wikidata':
                source_of_data = st.radio("Wählen Sie Ihre Datenquelle", ('','Private Data','Public Data'), horizontal=True, label_visibility="visible")
            else:
                source_of_data = st.radio("Wählen Sie Ihre Datenquelle", ('', 'Private Data','Public Data'), horizontal=True, label_visibility="visible")
            if source_of_data =='Private Data' and target_system =='Lobid (Standard)':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                st.write("Select your source of data")
                fn_private_data(data)
            elif source_of_data =='Public Data' and target_system =='Lobid (Standard)':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'True')]
                st.write("Select your source of data")
                fn_private_data(data)
            elif source_of_data =='Private Data' and target_system =='Lobid (Reconciliation API)':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                st.write("Select your source of data")
                fn_private_data_reconapi(data)
            elif source_of_data =='Public Data' and target_system =='Lobid (Reconciliation API)':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'True')]
                st.write("Select your source of data")
                fn_private_data_reconapi(data)
            elif source_of_data =='Public Data' and target_system == 'Wikidata':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'True')]
                st.write("Select your source of data")
                fun_wikidata_matching_scoring(data)
            elif source_of_data =='Private Data' and target_system == 'Wikidata':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                st.write("Select your source of data")
                fun_wikidata_matching_scoring(data)
            
        with tab2:
            target_system = st.radio("Wählen Sie bitte eine Zieldatenquelle",('Lobid-OSM', 'Wikidata'), horizontal=True, label_visibility="visible", key='building_matching_1')
            source_of_data = st.radio("Wählen Sie bitte eine Datenquelle", ('','Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key='building_matching_2')
            data=get_building_private_data()
            if source_of_data =='Private Data' and target_system == 'Lobid-OSM':
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                fn_buildings_private_data_lobid(data)
            elif source_of_data =='Public Data' and target_system == 'Lobid-OSM':
                data=data.loc[(data['public'] == 'True')]
                fn_buildings_private_data_lobid(data)
            elif source_of_data =='Private Data' and target_system == 'Wikidata':
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                fn_buildings_private_data_wikidata(data)
            elif source_of_data =='Public Data' and target_system == 'Wikidata':
                data=data.loc[(data['public'] == 'True')]
                fn_buildings_private_data_wikidata(data)
            
    elif select_option=="Data Import":
        st.header("Data Import")
        tab1, tab2 = st.tabs(["Private Data Import", "Csv Data Import"])
        with tab1:
            st.info("Um die Formatierungsanweisungen zu lesen, besuchen Sie bitte GitHub.")
            link = '[GitHub](https://github.com/MichaelMarkert/GND4C/tree/main/OpenRefine2JSON/)'
            st.markdown(link, unsafe_allow_html=True)
            fn_file_download()
            data_type = st.radio("Data Type", ('Person', 'Bauwerke'), horizontal=True, label_visibility="visible", key="data_type")
            save_type = st.radio("Save as", ('Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key="save_type")
            uploaded_file = st.file_uploader("Choose a file")
            file_extension=""
            if uploaded_file is not None:
                file_extension = pathlib.Path(uploaded_file.name).suffix
                if file_extension != '.json':
                    st.warning("You should choose correct file with extension (.json)")
                else:
                    pass
            else:
                pass
            if uploaded_file is not None and file_extension == '.json':
                now=datetime.now()
                date_of_import = now.strftime("%d/%m/%Y %H:%M:%S")
                st.info("Dataset definieren")
                imported_by=username
                st.write("Eingeführt von:", imported_by)
                st.write("Erstellt am:", date_of_import)
                default_title= date_of_import
                st.write("Default Title:", default_title)
                alternative_title = st.text_input("Geben Sie ein alternative Titel ein *", max_chars=55, key="alternative_title")
                if len(alternative_title)<= 0:
                    st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")
                comment_content = st.text_area("Geben Sie hier Ihre Kommentar ein", height=50)
                if len(comment_content)== 0:
                    comment_content = "None"
                bytes_data = uploaded_file.getvalue()
                data =json.loads(uploaded_file.read())
                conn=db_connect()
                if data_type == "Person" and 'gndo:differentiatedPerson' in data and len(alternative_title) > 0:
                    imported_data_save=person_data_upload(data)
                    data_id = uuid.uuid4()
                    data_id = str(data_id)
                    data_id = data_id[0:8]
                    date_of_import=now.strftime("%d/%m/%Y %H:%M:%S")
                    imported_data_save['data_id']= str(data_id) + '--' +str(date_of_import)
                    imported_data_save['default_title'] = str(default_title)
                    imported_data_save['imported_by'] = str(imported_by)
                    imported_data_save['user_id'] =str(user_id)
                    imported_data_save['date_of_import'] = str(date_of_import)
                    imported_data_save['alternative_title'] = str(alternative_title) + '--' +str(date_of_import)
                    imported_data_save['comment_content'] = str(comment_content)
                    if data_type == "Person" and save_type == "Public Data":
                        imported_data_save['public'] = str('True')
                        save = st.button("Save Database", key="person_public_data_save")
                        if save:
                            imported_data_save.to_sql(name='person_object_private_data', con=conn, if_exists='append', index=False, chunksize = 1000)
                            st.success('Ihre Daten wurden erfolgreich gespeichert.Bitte wechseln Sie zum "Matching" Teil.', icon="✅")
                    elif data_type == "Person" and save_type == "Private Data":
                        imported_data_save['public'] = str('False')
                        save = st.button("Save Database", key="person_private_data_save")
                        if save:
                            imported_data_save.to_sql(name='person_object_private_data', con=conn, if_exists='append', index=False, chunksize = 1000)
                            st.success('Ihre Daten wurden erfolgreich gespeichert.Bitte wechseln Sie zum "Matching" Teil.', icon="✅")
                elif data_type == "Bauwerke" and 'gndo:buildingOrMemorial' in data and len(alternative_title) > 0:
                    imported_data_save= building_data_upload(data) 
                    data_id = uuid.uuid4()
                    data_id = str(data_id)
                    data_id = data_id[0:8]
                    date_of_import=now.strftime("%d/%m/%Y %H:%M:%S")
                    imported_data_save['data_id']= str(data_id) + '--' +str(date_of_import)
                    imported_data_save['default_title'] = str(default_title)
                    imported_data_save['imported_by'] = str(imported_by)
                    imported_data_save['user_id'] =str(user_id)
                    imported_data_save['date_of_import'] = str(date_of_import)
                    imported_data_save['alternative_title'] = str(alternative_title) + '--' +str(date_of_import)
                    imported_data_save['comment_content'] = str(comment_content)
                    if data_type == "Bauwerke" and save_type == "Public Data":
                        imported_data_save['public'] = str('True')
                        save = st.button("Save Database", key="building_public_data_save")
                        if save:
                            imported_data_save.to_sql(name='building_object_private_data', con=conn, if_exists='append', index=False, chunksize = 1000)
                            st.success('Ihre Daten wurden erfolgreich gespeichert.Bitte wechseln Sie zum "Matching" Teil.', icon="✅")
                    elif data_type == "Bauwerke" and save_type == "Private Data":
                        imported_data_save['public'] = str('False')
                        save = st.button("Save Database", key="building_private_data_save")
                        if save:
                            imported_data_save.to_sql(name='building_object_private_data', con=conn, if_exists='append', index=False, chunksize = 1000)
                            st.success('Ihre Daten wurden erfolgreich gespeichert.Bitte wechseln Sie zum "Matching" Teil.', icon="✅")
                else:
                    st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")

        with tab2:
            fn_csv_file_download()
            csv_data_type = st.radio("Data Type", ('Person', 'Bauwerke'), horizontal=True, label_visibility="visible", key="csv_data_type")
            csv_save_type = st.radio("Save as", ('Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key="csv_save_type")
            st.info('Import your .csv file here.')
            uploaded_file = st.file_uploader("Choose a file", key='csv')
            
            file_extension=""
            if uploaded_file is not None:
                file_extension = pathlib.Path(uploaded_file.name).suffix
                if file_extension != '.csv':
                    st.warning("You should choose correct file with extension (.csv)")
                else:
                    pass
            else:
                pass
            if uploaded_file is not None and file_extension == '.csv':
                now=datetime.now()
                date_of_import = now.strftime("%d/%m/%Y %H:%M:%S")
                st.info("Dataset definieren")
                imported_by=username
                st.write("Eingeführt von:", imported_by)
                st.write("Erstellt am:", date_of_import)
                default_title= date_of_import
                st.write("Default Title:", default_title)
                alternative_title = st.text_input("Geben Sie ein alternative Titel ein *", max_chars=55, key="alternative_title_csv")
                if len(alternative_title)<= 0:
                    st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")
                if csv_data_type == "Person":
                    table_name =str("person_object_private_data")
                elif csv_data_type == "Bauwerke":
                    table_name =str("building_object_private_data")
                if len(table_name)<= 0:
                    st.warning("Please fill out the (*) mandatory fields.")
                comment_content = st.text_area("Enter your comment here", height=50)
                data=pd.read_csv(uploaded_file,sep=None)
                imported_by=username
                data_id = uuid.uuid4()
                data_id = str(data_id)
                data_id = data_id[0:8]
                data['imported_by']=str(imported_by)
                data['user_id'] =str(user_id)
                data['data_id']= str(data_id) + '--' +str(date_of_import)
                data['default_title'] = str(default_title)
                data['imported_by'] = str(imported_by)
                data['user_id'] =str(user_id)
                data['date_of_import'] = str(date_of_import)
                data['alternative_title'] = str(alternative_title) + '--' +str(date_of_import)
                data['comment_content'] = str(comment_content)
                
                if csv_save_type == "Private Data" and len(alternative_title) > 0:
                    data['public'] = str('False')
                    st.write(data)
                elif csv_save_type == "Public Data" and len(alternative_title) > 0:
                    data['public'] = str('True')
                    st.write(data)
                if st.button("Save Database") and len(alternative_title) > 0:
                    conn=db_connect()
                    data.to_sql(name=table_name, con=conn, if_exists='append', index=False, chunksize = 5000, method='multi')
                    st.success('Saved', icon="✅")
                else:
                    st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")

    elif select_option=="Ergebnisse":
        st.header("Ergebnisse")
        tab1, tab2 = st.tabs(["Ergebnisse für die Personen", "Ergebnisse für die Bauwerke"])
        with tab1:
            private_data_results()
        with tab2:
            building_data_result()

    elif select_option=="Reporting":
        st.header("Reporting")
        tab1, tab2 = st.tabs(["Reporting für die Personen", "Reporting für die Bauwerke"])
        data_type= st.radio("Data Type Type", ('Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key='reporting')
        with tab1:
            if data_type=='Public Data':
                st.write("This site is under construction.")
            elif data_type=='Private Data':
                st.write("This site is under construction.")
        with tab2:
            st.write("This site is under construction.")

    elif select_option =="Data Profiling":
        st.header("Data Profiling")
        tab1, tab2 = st.tabs(["Data Profiling für die Personen", "Data Profiling  für die Bauwerke"])        
        with tab1:
            source_of_data = st.radio("Wählen Sie bitte eine Datenquelle", ('','Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key='person')
            if source_of_data == 'Private Data':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'True') & (data['imported_by'] == username)]
                st.write("Select your source of data")
                fun_data_profiling_person(data)
            elif source_of_data == 'Public Data':
                data=get_person_private_data()
                data=data.loc[(data['public'] == 'False')]
                st.write("Select your source of data")
                fun_data_profiling_person(data)
        with tab2:
            source_of_data = st.radio("Wählen Sie bitte eine Datenquelle", ('','Private Data', 'Public Data'), horizontal=True, label_visibility="visible", key="building")
            if source_of_data =='Private Data':
                data=get_building_private_data()
                data=data.loc[(data['public'] == 'False') & (data['imported_by'] == username)]
                st.write("Select your source of data")
                fun_data_profiling_building(data)
            elif source_of_data =='Public Data':
                data=get_building_private_data()
                data=data.loc[(data['public'] == 'True')]
                st.write("Select your source of data")
                fun_data_profiling_building(data)

    elif select_option =="Data Visualisierung":
        st.header("Data Visualisierung")
        tab1, tab2 = st.tabs(["Visualisierung für die Personen", "Visualisierung für die Bauwerke"])
        with tab1:
            fun_visualize_person_data()
        with tab2:
            fun_visualize_building_data()
            
    elif select_option == "Blogpost":
        st.write("This site is under construction.")
        add_blog_post()

    elif select_option == "Benutzer Verwaltung":
        st.header("Benutzer Verwaltung")
        if user_role == "Admin" or user_role == "admin":
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["My User Details", "Update My User Details", "User Registration", "List Users", "Update User", "Delete User"])
            with tab1:
                st.subheader("My User Details")
                get_user_by_username(user_name)   
            with tab2:
                st.subheader("Update My User Details")
                update_single_user(user_id,user_role)
            with tab3:
                st.subheader("User Registration")
                user_registration()
            with tab4:
                list_users()
            with tab5:
                update_user()
            with tab6:
                delete_user()
        elif user_role == "Superuser":
            tab1, tab2, tab3, tab4, tab5 = st.tabs(["My User Details", "Update My User Details", "User Registration", "List Users", "Update User"])
            with tab1:
                st.subheader("My User Details")
                get_user_by_username(user_name)
            with tab2:
                st.subheader("Update My User Details")
                update_single_user(user_id,user_role)
            with tab3:
                st.subheader("User Registration")
                user_registration()
            with tab4:
                list_users()
            with tab5:
                update_user()
        elif user_role == "User":
            tab1, tab2 = st.tabs(["List My User Details", "Update My User Details"])
            with tab1:
                st.subheader("My User Details")
                get_user_by_username(user_name)
            with tab2:
                st.subheader("Update My User Details")
                update_single_user(user_id,user_role)