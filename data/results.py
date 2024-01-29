import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from st_aggrid import AgGrid, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import numpy as np
from data.agstyler import PINLEFT, PRECISION_TWO, draw_grid
import uuid
import time
import datetime
import datetime as dt
from datetime import datetime
import psycopg2
import requests
from users.session_state import session_state
from db.db_functions import db_connect_string


API_BASE_URL = "http://127.0.0.1:8000"
def source_private_data_results_import():
    response = requests.get(f"{API_BASE_URL}/get_queried_person_data")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")

def target_private_data_results_import():
    response = requests.get(f"{API_BASE_URL}/get_person_data_results")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")


def target_private_wikidata_results_import():
    response = requests.get(f"{API_BASE_URL}/get_person_wikidata_results")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        st.error("Failed to fetch data from the server.")


def source_building_data_import():
    try:
        conn_string = db_connect_string()
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()        
        data=pd.read_sql("SELECT * FROM queried_private_building_data", con=connection);
        return data

    except (Exception, psycopg2.Error) as error:
        st.write("Error in update operation", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def target_building_data_import():
    try:
        conn_string = db_connect_string()
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()        
        data=pd.read_sql("SELECT * FROM private_building_data_results", con=connection);
        return data

    except (Exception, psycopg2.Error) as error:
        st.write("Error in update operation", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def update_match_status(gndId, default_title, match_status, comment):
    try:
        conn_string = db_connect_string()
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()

        sql_select_query = """select * from private_person_data_results where gnd_id = %s and default_title = %s"""
        cursor.execute(sql_select_query, (gndId,default_title,))
        record = cursor.fetchone()

        sql_update_query = """Update private_person_data_results set match_status = %s where gnd_id = %s and default_title = %s"""
        cursor.execute(sql_update_query, (match_status,gndId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_update_query = """Update private_person_data_results set comment = %s where gnd_id = %s and default_title = %s"""
        cursor.execute(sql_update_query, (comment,gndId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_select_query = """select * from private_person_data_results where gnd_id = %s and default_title = %s"""
        cursor.execute(sql_select_query, (gndId,default_title,))
        record = cursor.fetchone()

    except (Exception, psycopg2.Error) as error:
        st.write("Error in update operation", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def update_match_status_building_data_with_wikidata(wikiId, default_title, match_status, comment):
    try:
        conn_string = db_connect_string()
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()

        sql_select_query = """select * from private_building_data_results where wikidata_id = %s and default_title = %s"""
        cursor.execute(sql_select_query, (wikiId,default_title,))
        record = cursor.fetchone()

        sql_update_query = """Update private_building_data_results set match_status = %s where wikidata_id = %s and default_title = %s"""
        cursor.execute(sql_update_query, (match_status,wikiId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_update_query = """Update private_building_data_results set comment = %s where wikidata_id = %s and default_title = %s"""
        cursor.execute(sql_update_query, (comment,wikiId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_select_query = """select * from private_building_data_results where wikidata_id = %s and default_title = %s"""
        cursor.execute(sql_select_query, (wikiId,default_title,))
        record = cursor.fetchone()

    except (Exception, psycopg2.Error) as error:
        st.write("Error in update operation", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def update_match_status_building_data_with_lobid(gndId, default_title, match_status, comment):
    try:
        conn_string = db_connect_string()
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()

        sql_select_query = """select * from private_building_data_results where gnd_id_lobid = %s and default_title = %s"""
        cursor.execute(sql_select_query, (gndId,default_title,))
        record = cursor.fetchone()

        sql_update_query = """Update private_building_data_results set match_status = %s where gnd_id_lobid = %s and default_title = %s"""
        cursor.execute(sql_update_query, (match_status,gndId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_update_query = """Update private_building_data_results set comment = %s where gnd_id_lobid = %s and default_title = %s"""
        cursor.execute(sql_update_query, (comment,gndId,default_title))
        connection.commit()
        count = cursor.rowcount

        sql_select_query = """select * from private_building_data_results where gnd_id_lobid = %s and default_title = %s"""
        cursor.execute(sql_select_query, (gndId,default_title,))
        record = cursor.fetchone()

    except (Exception, psycopg2.Error) as error:
        st.write("Error in update operation", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def lobid_data(source_data):
    st.info('Bitte Wählen Ihre Ergebnisse')
    with st.expander("Ergebnisse"):
        source_data['default_title']=source_data['default_title'].replace('nan', np.nan)
        source_data['default_title']=source_data['default_title'].fillna(method='ffill')

        with st.container():
            objects = st.container()
            object_list = source_data['default_title'].unique()
            choice = objects.selectbox("Choose a Dataset", object_list)
            st.info(f"Sie haben {choice} gewählt.")
            grouped = source_data.groupby(['default_title'])
            source_data = grouped.get_group(choice)
            query_data =[]
            info_table = pd.DataFrame(source_data, columns=['default_title','dc_id', 'person_id','data_history','source_system','entity_type', 'forename', 'surname', 'personal_name', 'name_addition', 'counting', 'prefix', 'non_preferred_name', 'preferred', 'gender', 'birthdate', 'deathdate', 'birthplace', 'deathplace', 'profession','descriptions'])         
            info_table['non_preferred_name']=np.where((info_table['non_preferred_name']=='nan'), 'None', info_table['non_preferred_name'])
            info_table = info_table.iloc[0:]
            info_table =pd.DataFrame(info_table)
            info_table_1 = info_table.transpose().astype(str)
            info_table_1 = info_table_1.drop([col for col in info_table_1.columns if info_table_1[col].eq('nan').any()], axis=1)
            info_table_1 = info_table_1.transpose()
            gd = GridOptionsBuilder.from_dataframe(info_table_1.astype(str))
            gd.configure_pagination(enabled=True)
            gd.configure_default_column(editable=True, groupable=True)
            gd.configure_column("dc_id", use_checkbox=True)
            gd.configure_selection(selection_mode= 'single', rowMultiSelectWithClick= True, use_checkbox=True)
            gridoptions = gd.build()
    st.info("Bitte wählen Sie eine NDS-ID aus")
    grid_table=AgGrid(info_table_1, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True)
    sel_row =grid_table["selected_rows"]
    sel_row=pd.DataFrame(sel_row, columns=['default_title','dc_id', 'person_id','data_history','source_system','entity_type', 'forename', 'surname', 'personal_name', 'name_addition', 'counting', 'prefix', 'non_preferred_name', 'preferred', 'gender', 'birthdate', 'deathdate', 'birthplace', 'deathplace', 'profession','descriptions'])

    sel_row_nds=sel_row.transpose().astype(str)
    sel_row=sel_row_nds.transpose().astype(str)
    selected_nds_id=sel_row['dc_id'].values.tolist()

    if len(selected_nds_id)<=0:
        st.warning("Bitte wählen Sie eine NDS-ID aus")
    elif len(selected_nds_id) > 0:
        selected_nds_id=selected_nds_id[0]
        selected_default_title=sel_row['default_title'].values.tolist()
        selected_default_title=selected_default_title[0]
        target_data=target_private_data_results_import()
        target_data=target_data.loc[target_data['default_title']==selected_default_title]
        source_data =pd.DataFrame(source_data, columns=['default_title','dc_id', 'person_id','data_history','source_system','entity_type', 'forename', 'surname', 'personal_name', 'name_addition', 'counting', 'prefix', 'non_preferred_name', 'preferred', 'gender', 'birthdate', 'deathdate', 'birthplace', 'deathplace', 'profession','descriptions'])
        source_data=source_data.sort_values(by = ['dc_id'], ascending = True)
        source_data_to_merge=pd.DataFrame(target_data, columns=['gnd_id','id','total_score','match_status','comment','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'IstMaxNameScore','IstMx_x_Anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score','activity_period_ijklmn_score', 'dc_id'])
        query_data = pd.merge(source_data_to_merge, source_data, on='dc_id', suffixes=('_target', '_source'), how='outer')
        target_data=target_data.loc[target_data['dc_id']==selected_nds_id]
        df2 = pd.DataFrame(target_data, columns=['default_title','gnd_id','id','total_score','match_status','comment','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'istmax_name_score','istmx_x_anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score','activity_period_ijklmn_score', 'nds_id','person_id'])
        df3 = pd.DataFrame(target_data, columns=['default_title','gnd_id','id','new_total_score', 'total_score','new_param_max_name_score','new_param_istmx_x_anz','new_param_istmax_name_score','new_param_job_score','new_param_g_birthdate_score','new_param_h_deathdate_score','new_param_o_birthplace_score','new_param_p_deathplace_score','new_param_activity_period_ijklmn_score','match_status','comment','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'istmax_name_score','istmx_x_anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score', 'activity_period_ijklmn_score','nds_id','person_id','query_urls'])
        st.info("Bitte definieren Sie Ihre neuen Bewertungsparameter.")
        agree = st.checkbox('Bearbeitbare Werte anzeigen')
        if agree:
            with st.expander("Bearbeitbare Werte"):       
                col_1, col_2, col_3 = st.columns([3,3,3])
    
        #if agree:
            
                with col_1:
                    new_base_shift = st.number_input('Neue new_base_shift einfügen', value=-8.41255)
                    st.write('Neue base_shift:', new_base_shift)

                    new_param_max_name_score = st.number_input('Neue param_max_name_score einfügen', value=1.298)
                    st.write('Neue param_max_name_score:', new_param_max_name_score)

                    new_param_IstMx_x_Anz = st.number_input('Neue param_IstMx_x_Anz einfügen', value=-0.283)
                    st.write('Neue param_IstMx_x_Anz:', new_param_IstMx_x_Anz)

                    new_param_IstMaxNameScore = st.number_input('Neue param_IstMaxNameScore einfügen', value=5.07)
                    st.write('Neue param_IstMaxNameScore:', new_param_IstMaxNameScore)
                with col_2:
                    new_param_job_score = st.number_input('Neue param_job_score einfügen', value=2.778)
                    st.write('Neue param_job_score:', new_param_job_score)

                    new_param_G_birthdate_score = st.number_input('Neue param_G_birthdate_score einfügen', value=1.139)
                    st.write('Neue param_G_birthdate_score:', new_param_G_birthdate_score)

                    new_param_H_deathdate_score = st.number_input('Neue param_H_deathdate_score einfügen', value=1.2015)
                    st.write('Neue param_H_deathdate_score:', new_param_H_deathdate_score)
                with col_3:
                    new_param_O_birthplace_score = st.number_input('Neue param_O_birthplace_score einfügen', value=0.857)
                    st.write('Neue param_O_birthplace_score:', new_param_O_birthplace_score)

                    new_param_P_deathplace_score = st.number_input('Neue param_P_deathplace_score einfügen', value=2.024)
                    st.write('Neue param_P_deathplace_score:', new_param_P_deathplace_score)

                    new_param_activity_period_ijklmn_score = st.number_input('Neue param_activity_period_ijklmn_score einfügen', value=0.178)
                    st.write('Neue param_activity_period_ijklmn_score:', new_param_activity_period_ijklmn_score)
            
                df3['new_param_max_name_score']=new_param_max_name_score*df3['max_name_score'].astype(float)
                df3['new_param_istmx_x_anz']=new_param_IstMx_x_Anz*df3['istmx_x_anz'].astype(float)
                df3['new_param_istmax_name_score']=new_param_IstMaxNameScore*df3['istmax_name_score'].astype(float)
                df3['new_param_job_score']=new_param_job_score*df3['max_job_score'].astype(float)
                df3['new_param_g_birthdate_score']=new_param_G_birthdate_score*df3['g_birthdate_score'].astype(float)
                df3['new_param_h_deathdate_score']=new_param_H_deathdate_score*df3['h_deathdate_score'].astype(float)
                df3['new_param_o_birthplace_score']=new_param_O_birthplace_score*df3['o_birthplace_score'].astype(float)
                df3['new_param_p_deathplace_score']=new_param_P_deathplace_score*df3['p_deathplace_score'].astype(float)
                df3['new_param_activity_period_ijklmn_score']=new_param_activity_period_ijklmn_score*df3['activity_period_ijklmn_score'].astype(float)
                df3['new_total_score']=new_base_shift + df3['new_param_max_name_score']+df3['new_param_istmx_x_anz']+df3['new_param_istmax_name_score']+df3['new_param_job_score']+df3['new_param_g_birthdate_score']+df3['new_param_h_deathdate_score']+df3['new_param_o_birthplace_score']+df3['new_param_p_deathplace_score']+df3['new_param_activity_period_ijklmn_score']


                df3['new_total_score'] =df3['new_total_score'].astype(float).round(decimals=2)
                df3['total_score'] =df3['total_score'].astype(float).round(decimals=2)
                df3=df3.sort_values(by='new_total_score', ascending=False)
            dropdownlst = ('Matched','Possible Match', 'No Match')
            
            gd_df3 = GridOptionsBuilder.from_dataframe(df3)
            gd_df3.configure_pagination(enabled=True)
            gd_df3.configure_default_column(editable=True, groupable=True)
            gd_df3.configure_column("match_status", editable=True, cellEditor='agSelectCellEditor', cellEditorParams={'values': dropdownlst})
            gd_df3.configure_column("id",
                                headerName="GND-URL",
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
            gd_df3.configure_column("new_total_score",cellStyle=cellsytle_jscode)
            gd_df3.configure_column("total_score",cellStyle=cellsytle_jscode)
            gd_df3.configure_column("comment", editable=True)
            gd_df3.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True, use_checkbox=True)
            gridoptions_df3 = gd_df3.build()

            st.info("Bitte wählen Sie eine GND Id aus")
            grid_table=AgGrid(df3, gridOptions=gridoptions_df3, update_mode="MANUAL", height = 500, allow_unsafe_jscode=True)
            sel_row =grid_table["selected_rows"]
            sel_row=pd.DataFrame(sel_row, columns=['default_title','gnd_id','id','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'new_total_score','total_score', 'match_status','comment', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'istmax_name_score','istmx_x_anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score', 'query_urls'])
            sel_row=sel_row.transpose()
        else:
            df3['new_total_score'] =df3['new_total_score'].astype(float).round(decimals=2)
            df3['total_score'] =df3['total_score'].astype(float).round(decimals=2)
            df3=df3.sort_values(by='total_score', ascending=False)
            
            dropdownlst = ('Matched','Possible Match', 'No Match')
            
            gd_df3 = GridOptionsBuilder.from_dataframe(df3)
            gd_df3.configure_pagination(enabled=True)
            gd_df3.configure_default_column(editable=True, groupable=True)
            gd_df3.configure_column("match_status", editable=True, cellEditor='agSelectCellEditor', cellEditorParams={'values': dropdownlst})
            gd_df3.configure_column("id",
                                headerName="GND-URL",
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
            gd_df3.configure_column("new_total_score",cellStyle=cellsytle_jscode)
            gd_df3.configure_column("total_score",cellStyle=cellsytle_jscode)
            gd_df3.configure_column("comment", editable=True)
            gd_df3.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True, use_checkbox=True)
            gridoptions_df3 = gd_df3.build()
            st.warning("Bitte wählen Sie eine ID aus")
            grid_table=AgGrid(df3, gridOptions=gridoptions_df3, update_mode= "MANUAL", height = 500, allow_unsafe_jscode=True)
            sel_row =grid_table["selected_rows"]
            sel_row=pd.DataFrame(sel_row, columns=['default_title','gnd_id','id','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'new_total_score','total_score', 'match_status','comment', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'istmax_name_score','istmx_x_anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score', 'query_urls'])
            sel_row=sel_row.transpose()
    else:
        st.warning("Bitte wählen Sie eine ID aus")
        grid_table=AgGrid(df3, gridOptions=gridoptions_df3, update_mode= "MANUAL", height = 500, allow_unsafe_jscode=True)
        sel_row =grid_table["selected_rows"]
        sel_row=pd.DataFrame(sel_row, columns=['default_title','gnd_id','id','preferred_name','variant_name', 'date_of_birth', 'date_of_death','place_of_birth', 'place_of_death', 'profession_or_occupation', 'new_total_score','total_score', 'match_status','comment', 'max_name_score', 'pref_name_score', 'var_name_score','non_pref_name_score','non_pref_var_name_score', 'istmax_name_score','istmx_x_anz', 'g_birthdate_score', 'h_deathdate_score','o_birthplace_score','p_deathplace_score', 'max_job_score', 'query_urls'])
        sel_row=sel_row.transpose()

    data_id = uuid.uuid4()
    data_id = str(data_id)
    data_id = data_id[0:8]
    data_id = [data_id]
    now=datetime.now()
    date_of_scoring=[str(now.strftime("%d/%m/%Y %H:%M:%S"))]
    gnd_id=" "
    gnd_id=[str(gnd_id)]
    data_id = pd.DataFrame(data_id, columns=['data_id'])
    date_of_scoring = pd.DataFrame(date_of_scoring, columns=['date_of_scoring'])
    gnd_id = pd.DataFrame(gnd_id, columns=['gnd_id'])
    meta_data=pd.concat([data_id, date_of_scoring,gnd_id], axis=1)


    col1, col2 = st.columns([2,3])
    with col1:
        st.info("Source Data")
        st.dataframe(sel_row_nds)

    with col2:
        st.info("GND Target Data")
        st.dataframe(sel_row.astype(str))
        data=pd.DataFrame(sel_row).astype(str).transpose()

    def convert_df(source_data):
        sel_row=sel_row_nds.transpose()
        df=pd.concat([meta_data, sel_row, data], axis=1)
        return df.to_csv(index=False).encode('utf-8')
    csv = convert_df(source_data)
    st.download_button("Drücken Sie, um ausgewählte Ergebnisse als .csv herunterzuladen", csv, "file.csv", "text/csv")

    def convert_data(query_data):
        query_data=pd.DataFrame(query_data)
        query_data=query_data.reset_index()
        return query_data.to_csv(index=True).encode('utf-8')
    csv = convert_data(query_data)
    st.download_button("Drücken Sie, um alle Ergebnisse als .csv herunterzuladen", csv, "file.csv", "text/csv")   

    if st.button("Zum Speichern drücken"):
        df=sel_row.transpose()
        for row in df.values:
            default_title = str(row[0])
            gnd_id = str(row[1])
            match_status= str(row[12])
            comment = str(row[13])
            update_match_status(gnd_id,default_title, match_status, comment)
        st.warning("Erfolgreich gespeichert")

def wiki_data(source_data):
    st.info('Bitte Wählen Ihre Ergebnisse')
    with st.expander("Ergebnisse"):
        source_data['default_title']=source_data['default_title'].replace('nan', np.nan)
        source_data['default_title']=source_data['default_title'].fillna(method='ffill')
        with st.container():
            objects = st.container()
            object_list = source_data['default_title'].unique()
            choice = objects.selectbox("Choose a Dataset", object_list)
            st.info(f"Sie haben {choice} gewählt.")
            grouped = source_data.groupby(['default_title'])
            source_data = grouped.get_group(choice)
            query_data =[]
            info_table = pd.DataFrame(source_data, columns=['default_title','dc_id', 'person_id','data_history','source_system','entity_type', 'forename', 'surname', 'personal_name', 'name_addition', 'counting', 'prefix', 'non_preferred_name', 'preferred', 'gender', 'birthdate', 'deathdate', 'birthplace', 'deathplace', 'profession','descriptions'])         
            info_table['non_preferred_name']=np.where((info_table['non_preferred_name']=='nan'), 'None', info_table['non_preferred_name'])
            info_table = info_table.iloc[0:]
            info_table =pd.DataFrame(info_table)
            info_table_1 = info_table.transpose().astype(str)
            info_table_1 = info_table_1.drop([col for col in info_table_1.columns if info_table_1[col].eq('nan').any()], axis=1)
            info_table_1 = info_table_1.transpose()
            gd = GridOptionsBuilder.from_dataframe(info_table_1.astype(str))
            gd.configure_pagination(enabled=True)
            gd.configure_default_column(editable=True, groupable=True)
            gd.configure_column("dc_id", use_checkbox=True)
            gd.configure_selection(selection_mode= 'single', rowMultiSelectWithClick= True, use_checkbox=True)
            gridoptions = gd.build()
    st.info("Bitte wählen Sie eine NDS-ID aus")
    grid_table=AgGrid(info_table_1, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True)
    sel_row =grid_table["selected_rows"]
    sel_row=pd.DataFrame(sel_row, columns=['default_title','dc_id', 'person_id','data_history','source_system','entity_type', 'forename', 'surname', 'personal_name', 'name_addition', 'counting', 'prefix', 'non_preferred_name', 'preferred', 'gender', 'birthdate', 'deathdate', 'birthplace', 'deathplace', 'profession','descriptions'])

    sel_row_nds=sel_row.transpose().astype(str)
    sel_row=sel_row_nds.transpose().astype(str)
    selected_nds_id=sel_row['dc_id'].values.tolist()

    if len(selected_nds_id)<=0:
        st.warning("Bitte wählen Sie eine ID aus")
    elif len(selected_nds_id) > 0:
        selected_nds_id=selected_nds_id[0]
        selected_default_title=sel_row['default_title'].values.tolist()
        selected_default_title=selected_default_title[0]
        target_data=target_data=target_private_wikidata_results_import()
        target_data=target_data.loc[target_data['default_title']==selected_default_title]
        target_data=target_data.loc[target_data['dc_id']==selected_nds_id]
        st.write(target_data)


def private_data_results():
    username=session_state.username
    data_type= st.radio("Data Type", ('Private Data', 'Public Data'), horizontal=True, label_visibility="visible",key="data_types")
    source_data_selected = st.radio("Target System", ('Lobid', 'Wikidata'), horizontal=True, label_visibility="visible",key="source_data")

    if data_type=='Private Data' and source_data_selected =='Lobid':
        source_data = source_private_data_results_import()      
        source_data=source_data.astype(str)
        list_of_values = [username, 'False']
        source_data[source_data[['queried_by','public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("queried_by in @list_of_values and public in @list_of_values")
        source_data=source_data.loc[source_data['target_data_source']=='lobid']
        lobid_data(source_data)
    elif data_type=='Public Data' and source_data_selected =='Lobid':
        source_data = source_private_data_results_import()      
        source_data=source_data.astype(str)
        list_of_values = ['True']
        source_data[source_data[['public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("public in @list_of_values")
        source_data=source_data.loc[source_data['target_data_source']=='lobid']
        lobid_data(source_data)
    elif data_type=='Public Data' and source_data_selected =='Wikidata':
        source_data = source_private_data_results_import()      
        source_data=source_data.astype(str)
        list_of_values = ['True']
        source_data[source_data[['public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("public in @list_of_values")
        source_data=source_data.loc[source_data['target_data_source']=='wikidata']
        wiki_data(source_data)
    elif data_type=='Private Data' and source_data_selected =='Wikidata':
        source_data = source_private_data_results_import()      
        source_data=source_data.astype(str)
        list_of_values = [username, 'False']
        source_data[source_data[['queried_by','public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("queried_by in @list_of_values and public in @list_of_values")
        source_data=source_data.loc[source_data['target_data_source']=='wikidata']
        wiki_data(source_data)

def building_data(source_data):
    st.info('Bitte Wählen Ihre Ergebnisse')
    with st.container():
        objects = st.container()
        object_list = source_data['default_title'].unique()
        choice = objects.selectbox("Choose a Dataset", object_list)
        st.write("choice", choice)
        grouped = source_data.groupby(['default_title'])
        source_data = grouped.get_group(choice)
        #source_data=source_data.reset_index()
        gd = GridOptionsBuilder.from_dataframe(source_data.astype(str))
        gd.configure_pagination(enabled=True)
        gd.configure_default_column(editable=True, groupable=True)
        gd.configure_column("dc_identifier", use_checkbox=True)
        gd.configure_selection(selection_mode= 'single', rowMultiSelectWithClick= True, use_checkbox=True)
        gridoptions = gd.build()
        st.info("Bitte wählen Sie eine ID aus")
        grid_table=AgGrid(source_data, gridOptions=gridoptions, update_mode="selection_changed", height = 500, allow_unsafe_jscode=True)
        sel_row =grid_table["selected_rows"]
        sel_row=pd.DataFrame(sel_row, columns=['dc_identifier', 'name','street_adress','place','lat_org', 'lon_org', 'display_name_org', 'date_of_query', 'default_title', 'alternative_title', 'comment_content'])
        sel_row_source=sel_row.transpose().astype(str)
        sel_row=sel_row_source.transpose().astype(str)
        selected_id=sel_row['dc_identifier'].values.tolist()
        query_data=[]
        if len(selected_id) == 0:
            st.warning("Bitte wählen Sie eine ID aus")
        elif len(selected_id) > 0:
            selected_id=selected_id[0]
            selected_default_title=sel_row['default_title'].values.tolist()
            selected_default_title=selected_default_title[0]
            target_data=target_building_data_import()
            target_data=target_data.loc[target_data['default_title']==selected_default_title]
            source_data_to_merge=pd.DataFrame(target_data, columns=['gnd_id_lobid', 'preferredName', 'display_name_org','display_name_target_geofabrik','total_score','match_status','comment','distance_score_lobid','distance_score_geofabrik', 'gnd_id_score', 'geo_distance_score','display_name_target_wikidata','wikidata_id','lev_distance_score_wikidata','lev_distance_wikidata', 'wikidata_coordinates', 'dms_lat','dms_lon','source_data_id','target_query_system'])
            source_data_to_merge=source_data_to_merge.rename({'source_data_id': 'id'}, axis=1)

            #source_data=source_data.rename({'dc_identifier': 'id'}, axis=1)

            query_data = pd.merge(source_data_to_merge, source_data, on='id', suffixes=('_target', '_source'), how='outer')
            target_data=target_data.loc[target_data['source_data_id']==selected_id]
            df2 = pd.DataFrame(target_data, columns=['default_title','gnd_id_lobid','preferredName','display_name_org','display_name_target_geofabrik','total_score','match_status','comment','distance_score_lobid','distance_score_geofabrik', 'gnd_id_score', 'geo_distance_score','display_name_target_wikidata_org','wikidata_id','lev_distance_score_wikidata','lev_distance_wikidata', 'wikidata_coordinates', 'dms_lat','dms_lon','source_data_id','target_query_system'])
            df2=df2.sort_values(by='total_score', ascending=False)
            dropdownlst = ('Matched','Possible Match', 'No Match')
            gd_df2 = GridOptionsBuilder.from_dataframe(df2)
            gd_df2.configure_pagination(enabled=True)
            gd_df2.configure_default_column(editable=True, groupable=True)
            gd_df2.configure_column("match_status", editable=True, cellEditor='agSelectCellEditor', cellEditorParams={'values': dropdownlst})
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
            gd_df2.configure_column("total_score",cellStyle=cellsytle_jscode)
            gd_df2.configure_column("comment", editable=True)
            gd_df2.configure_selection(selection_mode= 'multiple', rowMultiSelectWithClick= True, use_checkbox=True)
            gridoptions_df2 = gd_df2.build()
            st.info("Bitte wählen Sie eine Id aus")
            grid_table=AgGrid(df2, gridOptions=gridoptions_df2, update_mode="MANUAL", height = 500, allow_unsafe_jscode=True)
            sel_row =grid_table["selected_rows"]
            sel_row=pd.DataFrame(sel_row, columns=['default_title','gnd_id_lobid','preferredName','display_name_target_geofabrik','total_score','match_status','comment','distance_score_lobid','distance_score_geofabrik', 'gnd_id_score', 'geo_distance_score','display_name_target_wikidata_org','wikidata_id','lev_distance_score_wikidata','lev_distance_wikidata', 'wikidata_coordinates', 'dms_lat','dms_lon','source_data_id','target_query_system'])
            
            sel_row=sel_row.transpose()
            data_id = uuid.uuid4()
            data_id = str(data_id)
            data_id = data_id[0:8]
            data_id = [data_id]
            now=datetime.now()
            date_of_scoring=[str(now.strftime("%d/%m/%Y %H:%M:%S"))]
            gnd_id=" "
            gnd_id=[str(gnd_id)]
            data_id = pd.DataFrame(data_id, columns=['data_id'])
            date_of_scoring = pd.DataFrame(date_of_scoring, columns=['date_of_scoring'])
            gnd_id = pd.DataFrame(gnd_id, columns=['gnd_id'])
            meta_data=pd.concat([data_id, date_of_scoring,gnd_id], axis=1)
            col1, col2 = st.columns([2,3])
            with col1:
                st.info("Source Data")
                st.dataframe(sel_row_source)
            with col2:
                st.info("Target Data")
                st.dataframe(sel_row.astype(str))
                selected=sel_row
                data=pd.DataFrame(sel_row).astype(str).transpose()
            def convert_df(source_data):
                sel_row=sel_row_source.transpose()
                df=pd.concat([meta_data, sel_row, data], axis=1)
                return df.to_csv(index=False).encode('utf-8')
            csv = convert_df(source_data)
            st.download_button("Drücken Sie, um ausgewählte Ergebnisse als .csv herunterzuladen", csv, "file.csv", "text/csv", key='building_1')

            def convert_data(query_data):
                query_data=pd.DataFrame(query_data)
                return query_data.to_csv(index=True).encode('utf-8')
            csv = convert_data(query_data)
            st.download_button("Drücken Sie, um alle Ergebnisse als .csv herunterzuladen", csv, "file.csv", "text/csv", key='building_2')   
            if st.button("Zum Speichern drücken", key='building_data'):
                df=pd.DataFrame(sel_row).astype(str).transpose()
                target_query_system=df['target_query_system'][0]
                if target_query_system =="wikidata":
                    for row in df.values:
                        default_title = str(row[0])
                        wikidata_id = str(row[12])
                        match_status= str(row[5])
                        comment = str(row[6])
                        update_match_status_building_data_with_wikidata(wikidata_id, default_title, match_status, comment)
                    st.warning("Erfolgreich gespeichert")
                elif target_query_system =="lobid_osm":
                    for row in df.values:
                        default_title = str(row[0])
                        gnd_id_lobid = str(row[1])
                        match_status= str(row[5])
                        comment = str(row[6])
                        update_match_status_building_data_with_lobid(gnd_id_lobid, default_title, match_status, comment)
                    st.warning("Erfolgreich gespeichert")

def building_data_result():
    username=session_state.username
    data_type= st.radio("Data Type", ('Private Data', 'Public Data'), horizontal=True, label_visibility="visible",key="data_types_building")
    source_data_selected = st.radio("Target System", ('Lobid-OSM', 'Wikidata'), horizontal=True, label_visibility="visible",key="source_data_building")

    if data_type=='Private Data' and source_data_selected =='Lobid-OSM':
        source_data = source_building_data_import()   
        source_data=source_data.astype(str)
        list_of_values = [username, 'False']
        source_data[source_data[['queried_by','public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("queried_by in @list_of_values and public in @list_of_values")
        source_data=source_data.loc[source_data['target_query_system']=='lobid_osm']
        building_data(source_data)
    elif data_type=='Public Data' and source_data_selected =='Lobid-OSM':
        source_data = source_building_data_import()     
        source_data=source_data.astype(str)
        list_of_values = ['True']
        source_data[source_data[['public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("public in @list_of_values")
        source_data=source_data.loc[source_data['target_query_system']=='lobid_osm']
        building_data(source_data)
    elif data_type=='Public Data' and source_data_selected =='Wikidata':
        source_data = source_building_data_import()      
        source_data=source_data.astype(str)
        list_of_values = ['True']
        source_data[source_data[['public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("public in @list_of_values")
        source_data=source_data.loc[source_data['target_query_system']=='wikidata']
        building_data(source_data)
    elif data_type=='Private Data' and source_data_selected =='Wikidata':
        source_data = source_building_data_import()     
        source_data=source_data.astype(str)
        list_of_values = [username, 'False']
        source_data[source_data[['queried_by','public']].isin(list_of_values).all(1)] 
        source_data=source_data.query("queried_by in @list_of_values and public in @list_of_values")
        source_data=source_data.loc[source_data['target_query_system']=='wikidata']
        building_data(source_data)

