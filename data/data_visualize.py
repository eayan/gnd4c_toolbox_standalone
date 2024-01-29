import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid import JsCode
import numpy as np
import pygwalker as pyg
from data.results import source_building_data_import, target_building_data_import, source_private_data_results_import, target_private_data_results_import,target_private_wikidata_results_import
from users.session_state import session_state


def lobid_data(source_data):
    st.info('Bitte Wählen Ihre Ergebnisse')

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
        target_data['alternative_title']=target_data['alternative_title'].astype(str)
        target_data['comment_content']=target_data['comment_content'].astype(str)
        target_data['total_score']=target_data['total_score'].astype(float).round(decimals=2)
        target_data['non_pref_name_score']=target_data['non_pref_name_score'].astype(float).round(decimals=2)
        target_data['non_pref_var_name_score']=target_data['non_pref_var_name_score'].astype(float).round(decimals=2)

        target_data['total_score'] = target_data['total_score'].astype(float).round(decimals=2)
        def load_config(file_path):
            with open(file_path, 'r') as config_file:
                config_str = config_file.read()
                return config_str
        config= load_config('config/config.json') # Replace with your own folder paths
        pyg.walk(target_data, env='Streamlit', dark='light', spec=config)

def wiki_data(source_data):
    st.info('Bitte Wählen Ihre Ergebnisse')

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
    st.info("Bitte wählen Sie eine ID aus")
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
            
def fun_visualize_person_data(): 
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
    with st.container():
        objects = st.container()
        object_list = source_data['default_title'].unique()
        choice = objects.selectbox("Choose a Dataset", object_list)
        st.write("choice", choice)
        grouped = source_data.groupby(['default_title'])
        source_data = grouped.get_group(choice)
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
            query_data = pd.merge(source_data_to_merge, source_data, on='id', suffixes=('_target', '_source'), how='outer')
            target_data=target_data.loc[target_data['source_data_id']==selected_id]
            target_data['source_data_id']=target_data['source_data_id'].astype(str)
            target_data['gnd_id_lobid']=target_data['gnd_id_lobid'].astype(str)
            target_data['alternative_title']=target_data['alternative_title'].astype(str)
            target_data['comment_content']=target_data['comment_content'].astype(str)
            target_data['total_score']=target_data['total_score'].astype(float).round(decimals=2)
            target_data['total_score'] = target_data['total_score'].astype(float).round(decimals=2)
            def load_config(file_path):
                with open(file_path, 'r') as config_file:
                    config_str = config_file.read()
                    return config_str
            config= load_config('config/config_building.json') # Replace with your own folder paths
            pyg.walk(target_data, env='Streamlit', dark='light', spec=config)

def fun_visualize_building_data():
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