import requests
import urllib.request
from urllib.request import urlopen
import json
import pandas as pd
import streamlit as st
import numpy as np
from glom import glom
from ast import literal_eval
import os
from sqlalchemy import create_engine
import uuid
import time
import datetime
import datetime as dt
from datetime import datetime
import string
import psycopg2

def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text

def fn_file_download():
    person_f = open ('documents/sample_json_template.json', "r")
    data = json.loads(person_f.read())
    json_string = json.dumps(data)
    st.info("Sie können die JSON-Beispieldaten für Person Data herunterladen und Ihre eigenen privaten Daten entsprechend organisieren.")
    st.download_button(label="Download Person Data", file_name="data.json", mime="application/json", data=json_string, key="person")

    buildings_f = open ('documents/Dehio-D-Brandenburg_sample.json', "r")
    data = json.loads(buildings_f.read())
    json_string = json.dumps(data)
    st.info("Sie können die JSON-Beispieldaten für Bauwerke Data herunterladen und Ihre eigenen privaten Daten entsprechend organisieren.")
    st.download_button(label="Download Bauwerke Data", file_name="data.json", mime="application/json", data=json_string, key="bauwerke")

def convert_df(df):
    return df.to_csv().encode('utf-8')

def fn_csv_file_download():
    data_person=pd.read_csv('documents/sample_private_person_data.csv')
    csv_person=convert_df(data_person)
    st.info("Sie können die CSV-Beispieldaten für Person Data herunterladen und Ihre eigenen privaten Daten entsprechend organisieren.")
    st.download_button(label="Download Person Data", file_name="sample_person_data.csv", mime='text/csv', data=csv_person, key="csv_person")

    data_building=pd.read_csv('documents/sample_private_building_data.csv')
    csv_building=convert_df(data_building)
    st.info("Sie können die CSV-Beispieldaten für Bauwerke Data  herunterladen und Ihre eigenen privaten Daten entsprechend organisieren.")
    st.download_button(label="Download Bauwerke Data", file_name="sample_building_data.csv", mime='text/csv', data=csv_building, key="csv_bauwerke")

def convert_df(df_nested_list_data):
    return df_nested_list_data.to_csv(index=False).encode('utf-8')

def person_data_upload(data):
    imported_data = pd.json_normalize(data['gndo:differentiatedPerson'])
    imported_data =imported_data.drop_duplicates(subset=['dc:identifier'],keep='first')

    preferred_name_data=pd.DataFrame(imported_data,columns=['dc:identifier','gndo:preferredNameOfThePerson.gndo:forename', 'gndo:preferredNameOfThePerson.gndo:surname','gndo:preferredNameOfThePerson.gndo:personalName','gndo:preferredNameOfThePerson.gndo:nameAddition','gndo:preferredNameOfThePerson.gndo:prefix','gndo:preferredNameOfThePerson.gndo:counting','gndo:gender.rdfs:literal', 'gndo:dateOfBirth.schema:date','gndo:dateOfDeath.schema:date'])
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].astype(str)
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].str.replace("|",'')
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].str.replace("1813-01-011813-03-27",'1813')
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].str.replace("1810-01-011811-11-20",'1810')
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].str.replace("1821-01-011825-01-01",'1821')
    preferred_name_data['gndo:dateOfDeath.schema:date']=preferred_name_data['gndo:dateOfDeath.schema:date'].str.replace("1801-01-011847-11-18",'1801')
    
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].astype(str)
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].str.replace("|",'')
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].str.replace("1736-01-151738-01-01",'1736')
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].str.replace("1736-01-011737-01-01",'1736')
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].str.replace("1747-01-011748-12-17",'1747')
    preferred_name_data['gndo:dateOfBirth.schema:date']=preferred_name_data['gndo:dateOfBirth.schema:date'].str.replace("1759-01-011762-01-01",'1759')

    variant_name_data= imported_data.explode('gndo:variantNameOfThePerson')
    profession_data = imported_data.explode('gndo:professionOrOccupation')
    place_of_birth_data = imported_data.explode('gndo:placeOfBirth')
    place_of_death_data = imported_data.explode('gndo:placeOfDeath')
    period_of_activity_data = imported_data.explode('gndo:periodOfActivity')

    imported_data_save=pd.concat([imported_data], axis=1)
    imported_data_save = pd.DataFrame(imported_data_save).astype(str)
    imported_data_save =imported_data_save.sort_values(by = ['dc:identifier'], ascending = True)

    preferred_name=preferred_name_data.iloc[0:]
    preferred_name=pd.DataFrame(preferred_name).astype(str)
    preferred_name['gndo:preferredNameOfThePerson.gndo:forename']=preferred_name['gndo:preferredNameOfThePerson.gndo:forename'].str.replace('\d+', '').str.replace('-',' ')
    preferred_name['gndo:preferredNameOfThePerson.gndo:forename']=preferred_name['gndo:preferredNameOfThePerson.gndo:forename'].str.replace('\d+', '').str.replace('[^\w\s]','')
    preferred_name['gndo:preferredNameOfThePerson.gndo:surname']=preferred_name['gndo:preferredNameOfThePerson.gndo:surname'].str.replace('-', ' ')
    preferred_name['gndo:preferredNameOfThePerson.gndo:surname']=preferred_name['gndo:preferredNameOfThePerson.gndo:surname'].str.replace('\d+', '').str.replace('[^\w\s]','')
    preferred_name['gndo:preferredNameOfThePerson.gndo:personalName']=preferred_name['gndo:preferredNameOfThePerson.gndo:personalName'].str.replace('\d+', '').str.replace('-',' ')
    preferred_name['gndo:preferredNameOfThePerson.gndo:personalName']=preferred_name['gndo:preferredNameOfThePerson.gndo:personalName'].str.replace('\d+', '').str.replace('[^\w\s]','')
    preferred_name['gndo:preferredNameOfThePerson.gndo:nameAddition']=preferred_name['gndo:preferredNameOfThePerson.gndo:nameAddition'].str.replace('\d+', '').str.replace('-',' ')
    preferred_name['gndo:preferredNameOfThePerson.gndo:nameAddition']=preferred_name['gndo:preferredNameOfThePerson.gndo:nameAddition'].str.replace('\d+', '').str.replace('[^\w\s]','')            
    preferred_name=preferred_name.rename(columns={"dc:identifier": "dc:id", "gndo:preferredNameOfThePerson.gndo:forename":"forename", "gndo:preferredNameOfThePerson.gndo:surname":"surname","gndo:preferredNameOfThePerson.gndo:personalName":"personal_name", "gndo:preferredNameOfThePerson.gndo:nameAddition":"name_addition", "gndo:preferredNameOfThePerson.gndo:prefix":"prefix", "gndo:preferredNameOfThePerson.gndo:counting":"counting","gndo:gender.rdfs:literal":"gender","gndo:dateOfBirth.schema:date":"birth_date","gndo:dateOfDeath.schema:date":"death_date"})

    period_of_activity=period_of_activity_data.iloc[0:]
    period_of_activity=pd.DataFrame(period_of_activity, columns=['dc:identifier','gndo:periodOfActivity']).astype(str)
    period_of_activity['activity_period']=period_of_activity['gndo:periodOfActivity'].str.replace("{'schema:date': None, 'rdfs:literal': '",'')
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("{'schema:date': None, 'rdfs:literal': None}",'')
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("'}",'')
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("{'schema:date': '",'')
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("', 'rdfs:literal': None}",'')
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("', 'rdfs:literal': None}",'')
    period_of_activity['activity_period']= period_of_activity['activity_period'].str.findall('(\d{4})').astype(str)
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("[], []",'').str.strip()
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace("'",' ').str.strip()
    period_of_activity['activity_period']=period_of_activity['activity_period'].str.replace(",",' ').str.strip()
    period_of_activity['activity_period'] = period_of_activity['activity_period'].apply(remove_punctuations)
    period_of_activity['period_of_activity_length']= period_of_activity['activity_period'].str.len()
    period_of_activity['activity_period'] = np.where((period_of_activity['period_of_activity_length'] <=1), 'None None', period_of_activity['activity_period'])

    period_of_activity=pd.DataFrame(period_of_activity).astype(str)
    period_of_activity_1 = period_of_activity.groupby(['dc:identifier']).agg(lambda col: ' '.join(map(str, col)))
    period_of_activity_1=pd.DataFrame(period_of_activity_1, columns=['activity_period']).astype(str)

    period_of_activity_1[['period_of_activity_start', 'period_of_activity_end']] = period_of_activity_1['activity_period'].str.split(' ', 1, expand=True)
    period_of_activity_1['period_of_activity_start']=period_of_activity_1['period_of_activity_start'].str[:5]

    period_of_activity_1['period_of_activity_start_length']= period_of_activity_1['period_of_activity_start'].str.len()
    period_of_activity_1['period_of_activity_start'] = np.where((period_of_activity_1['period_of_activity_start_length'] <=1), 'None', period_of_activity_1['period_of_activity_start'])

    period_of_activity_1['period_of_activity_end']=period_of_activity_1['period_of_activity_end'].str[:5]
    period_of_activity_1['period_of_activity_end_length']=period_of_activity_1['period_of_activity_end'].str.len()
    period_of_activity_1['period_of_activity_end'] = np.where((period_of_activity_1['period_of_activity_end_length'] <=1), 'None', period_of_activity_1['period_of_activity_end'])

    place_of_death=place_of_death_data.iloc[0:]
    place_of_death=pd.DataFrame(place_of_death, columns=['dc:identifier','gndo:placeOfDeath']).astype(str)
    place_of_death['death_place']=place_of_death['gndo:placeOfDeath'].apply(lambda st: st[st.find("'rdfs:literal': '"):st.find("', 'gndo:gndIdentifier':")])
    place_of_death['death_place']=place_of_death['death_place'].str.replace("'rdfs:literal': '", '')
    place_of_death['death_place']=place_of_death['death_place'].str.replace("<O>\n",'')
    place_of_death['death_place']=place_of_death['death_place'].str.replace("<O>",'')
    place_of_death['death_place']=place_of_death['death_place'].str.replace("\n",'')
    place_of_death['death_place']=place_of_death['death_place'].apply(remove_punctuations)
    place_of_death['death_place']=place_of_death['death_place'].str.replace(" n",'')

    place_of_death['death_place_length']= place_of_death['death_place'].str.len()
    place_of_death['death_place'] = np.where((place_of_death['death_place_length'] <=1), 'Not Described', place_of_death['death_place'])
    place_of_death=pd.DataFrame(place_of_death)
    place_of_death_1 = place_of_death.groupby(['dc:identifier']).agg(lambda col: ', '.join(map(str, col)))
    place_of_death_1 = place_of_death_1['death_place'].str.split(',', expand=True)
    place_of_death_1['death_place']=place_of_death_1[0]

    place_of_birth=place_of_birth_data.iloc[0:]
    place_of_birth=pd.DataFrame(place_of_birth, columns=['dc:identifier','gndo:placeOfBirth']).astype(str)
    place_of_birth['birth_place']=place_of_birth['gndo:placeOfBirth'].apply(lambda st: st[st.find("'rdfs:literal': '"):st.find("', 'gndo:gndIdentifier':")])
    place_of_birth['birth_place']=place_of_birth['birth_place'].str.replace("'rdfs:literal': '", '')
    place_of_birth['birth_place']=place_of_birth['birth_place'].str.replace("<O>\n",'')
    place_of_birth['birth_place']=place_of_birth['birth_place'].str.replace("<O>",'')
    place_of_birth['birth_place']=place_of_birth['birth_place'].str.replace("\n",'')
    place_of_birth['birth_place']=place_of_birth['birth_place'].apply(remove_punctuations)
    place_of_birth['birth_place']=place_of_birth['birth_place'].str.replace(" n",'')

    place_of_birth['birth_place_length']= place_of_birth['birth_place'].str.len()
    place_of_birth['birth_place'] = np.where((place_of_birth['birth_place_length'] <=1), 'Not Described', place_of_birth['birth_place'])
    place_of_birth=pd.DataFrame(place_of_birth)
    place_of_birth_1 = place_of_birth.groupby(['dc:identifier']).agg(lambda col: ', '.join(map(str, col)))
    place_of_birth_1 = place_of_birth_1['birth_place'].str.split(',', expand=True)
    place_of_birth_1['birth_place']=place_of_birth_1[0]

    variant_name=variant_name_data.iloc[0:]
    variant_name=pd.DataFrame(variant_name, columns=['dc:identifier','gndo:variantNameOfThePerson']).astype(str)
    variant_name['var_forename'] =variant_name['gndo:variantNameOfThePerson'].apply(lambda st: st[st.find("'gndo:forename':"):st.find(", 'gndo:surname':")])
    variant_name['var_surname'] =variant_name['gndo:variantNameOfThePerson'].apply(lambda st: st[st.find("'gndo:surname':"):st.find(", 'gndo:personalName':")])
    variant_name['var_forename'] =variant_name['var_forename'].str.replace("'gndo:forename':", '')
    variant_name['var_forename'] =variant_name['var_forename'].str.replace("'", '')
    variant_name['var_forename'] =variant_name['var_forename'].str.strip()
    variant_name['var_forename'] =variant_name['var_forename'].replace('\\n','', regex=True)
    variant_name['var_forename'] =variant_name['var_forename'].replace('>\n','', regex=True)
    variant_name['var_forename'] =variant_name['var_forename'].replace('>','', regex=True)
    variant_name['var_forename'] =variant_name['var_forename'].replace(',','', regex=True)
    variant_name['var_forename'] =variant_name['var_forename'].replace('<','', regex=True) 
    variant_name['var_forename'] =variant_name['var_forename'].str.replace('[^\w\s]','')
    variant_name['var_surname']=variant_name['var_surname'].str.replace("'gndo:surname':", '')
    variant_name['var_surname']=variant_name['var_surname'].str.replace("'", '')
    variant_name['var_surname'] =variant_name['var_surname'].replace('\\n','', regex=True)
    variant_name['var_surname'] =variant_name['var_surname'].replace('>\n','', regex=True)
    variant_name['var_surname'] =variant_name['var_surname'].replace('>','', regex=True)
    variant_name['var_surname'] =variant_name['var_surname'].replace(',','', regex=True)
    variant_name['var_surname'] =variant_name['var_surname'].replace('<','', regex=True) 
    variant_name['var_surname'] =variant_name['var_surname'].str.replace('[^\w\s]','')

    variant_name = pd.DataFrame(variant_name, columns=['dc:identifier','var_forename', 'var_surname'])
    variant_name['non_preferred_name_surname'] = variant_name['var_forename'] + variant_name['var_surname']
    variant_name['non_preferred_name_surname']=variant_name['non_preferred_name_surname'].apply(lambda x : x.replace(',',''))
    variant_name=pd.DataFrame(variant_name)
    variant_name_1=variant_name.groupby(['dc:identifier']).agg(lambda col: ', '.join(map(str, col)))

    profession=profession_data.iloc[0:]
    
    profession=pd.DataFrame(profession, columns=['dc:identifier','gndo:professionOrOccupation']).astype(str)
    profession['profession']=profession['gndo:professionOrOccupation'].apply(lambda st: st[st.find("'rdfs:literal': '"):st.find("', 'gndo:gndIdentifier'")])
    profession['profession']=profession['profession'].str.replace("'rdfs:literal': '", '')
    profession['profession_length']= profession['profession'].str.len()
    profession['profession'] = np.where((profession['profession_length'] <=1), 'None', profession['profession'])
    profession=pd.DataFrame(profession)
    profession_1 = profession.groupby(['dc:identifier']).agg(lambda col: ', '.join(map(str, col)))
    
    rest_of_data=pd.concat([variant_name_1['non_preferred_name_surname'],place_of_birth_1['birth_place'], place_of_death_1['death_place'], period_of_activity_1['period_of_activity_start'],period_of_activity_1['period_of_activity_end'],profession_1['profession']], axis=1)
    rest_of_data = pd.DataFrame(rest_of_data).astype(str)
    rest_of_data =rest_of_data.reset_index()
    preferred_name = preferred_name.sort_values(by = ['dc:id'], ascending = True).reset_index()
    rest_of_data =rest_of_data.sort_values(by = ['dc:identifier'], ascending = True)

    imported_data_save=pd.concat([preferred_name,rest_of_data], axis=1)
    imported_data_save = imported_data_save.rename({'dc:id': 'dc_id'}, axis=1)
    imported_data_save = imported_data_save.rename({'dc:identifier': 'dc_identifier'}, axis=1)
    
    st.write(imported_data_save.astype(str))
    csv = convert_df(imported_data_save) 
    st.download_button("Zum Herunterladen drücken", csv, "file.csv", "text/csv")
    return imported_data_save

def building_data_upload(data):
    imported_data = pd.json_normalize(data['gndo:buildingOrMemorial'])
    buildings_data=pd.DataFrame(imported_data,columns=['dc:identifier','gndo:gndIdentifier','schema:alternateName','gndo:place.gndo:abbreviatedNameForThePlaceOrGeographicName','gndo:placeOrGeographicName','gndo:dateOfProduction.schema:date','gndo:preferredNameForThePlaceOrGeographicName','gndo:variantNameForThePlaceOrGeographicName','gndo:relatedPlaceOrGeographicName', 'gndo:professionalRelationship','gndo:biographicalOrHistoricalInformation', 'gndo:gndSubjectCategory', 'gndo:relatedSubjectHeading', 'gndo:geographicAreaCode','gndo:place.rdfs:literal', 'gndo:place.gndo:gndIdentifier', 'gndo:hierarchicalSuperiorOfPlaceOrGeographicName.rdfs:literal','schema:address.@type', 'schema:address.schema:streetAddress','schema:address.schema:postalCode','schema:geo.@type', 'schema:geo.schema:latitude', 'schema:geo.schema:longitude', 'gndo:broaderTermInstantial.rdfs:literal', 'gndo:broaderTermInstantial.gndo:gndIdentifier'])
    relatedPlaceOrGeographicName_data = buildings_data.explode('gndo:relatedPlaceOrGeographicName')
    professionalRelationship_data = buildings_data.explode('gndo:professionalRelationship')
    gndSubjectCategory_data = buildings_data.explode('gndo:gndSubjectCategory')
    relatedSubjectHeading_data = buildings_data.explode('gndo:relatedSubjectHeading')

    imported_data_save=pd.concat([buildings_data], axis=1)
    imported_data_save = pd.DataFrame(imported_data_save).astype(str)
    imported_data_save =imported_data_save.sort_values(by = ['dc:identifier'], ascending = True)
    
    relatedSubjectHeading=relatedSubjectHeading_data.iloc[0:]
    relatedSubjectHeading=pd.DataFrame(relatedSubjectHeading, columns=['gndo:relatedSubjectHeading']).astype(str)
    relatedSubjectHeading['relatedSubjectHeading']=relatedSubjectHeading['gndo:relatedSubjectHeading'].apply(lambda st: st[st.find("'@type': 'LandmarksOrHistoricalBuildings', 'rdfs:literal':"):st.find(", 'gndo:gndIdentifier': None, 'schema:description': None")])
    relatedSubjectHeading['relatedSubjectHeading']=relatedSubjectHeading['relatedSubjectHeading'].str.replace("'@type': 'LandmarksOrHistoricalBuildings', 'rdfs:literal': ", '')

    professionalRelationship=professionalRelationship_data.iloc[0:]
    professionalRelationship=pd.DataFrame(professionalRelationship, columns=['gndo:professionalRelationship']).astype(str)
    professionalRelationship['professionalRelationship']=professionalRelationship['gndo:professionalRelationship'].apply(lambda st: st[st.find("'gndo:preferredNameOfThePerson': {'@type':"):st.find("', 'gndo:forename':")])
    professionalRelationship['professionalRelationship']=professionalRelationship['professionalRelationship'].str.replace("'gndo:preferredNameOfThePerson': {'@type': '", '')

    relatedPlaceOrGeographicName=relatedPlaceOrGeographicName_data.iloc[0:]
    relatedPlaceOrGeographicName=pd.DataFrame(relatedPlaceOrGeographicName, columns=['dc:identifier','gndo:relatedPlaceOrGeographicName']).astype(str)
    relatedPlaceOrGeographicName['relatedPlaceOrGeographicName']=relatedPlaceOrGeographicName['gndo:relatedPlaceOrGeographicName'].apply(lambda st: st[st.find("'rdfs:literal':"):st.find("', 'gndo:gndIdentifier':")])
    relatedPlaceOrGeographicName['relatedPlaceOrGeographicName']=relatedPlaceOrGeographicName['relatedPlaceOrGeographicName'].str.replace("'rdfs:literal':", '')
    relatedPlaceOrGeographicName['relatedPlaceOrGeographicName']=relatedPlaceOrGeographicName['relatedPlaceOrGeographicName'].str.replace(", 'gndo:gndIdentifier': None", '')

    gndSubjectCategory=gndSubjectCategory_data.iloc[0:]
    gndSubjectCategory=pd.DataFrame(gndSubjectCategory, columns=['dc:identifier','gndo:gndSubjectCategory']).astype(str)
    gndSubjectCategory['subject_category'] =gndSubjectCategory['gndo:gndSubjectCategory'].apply(lambda st: st[st.find("'rdfs:literal':"):st.find("', 'gndo:gndIdentifier':")])
    gndSubjectCategory['subject_category'] =gndSubjectCategory['subject_category'].str.replace("'rdfs:literal':", '')  
    gndSubjectCategory['subject_category'] =gndSubjectCategory['subject_category'].str.replace("None, 'gndo:gndIdentifier': None", 'None')
    gndSubjectCategory['subject_category'] =gndSubjectCategory['subject_category'].str.replace("'", '')
    gndSubjectCategory['subject_category']=gndSubjectCategory.groupby(['dc:identifier'])['subject_category'].transform(lambda x: ', '.join(x))
    gndSubjectCategory =gndSubjectCategory.drop_duplicates(subset=['dc:identifier'],keep='first')
    
    #gndSubjectCategory['subject_category'] = gndSubjectCategory['subject_category'].apply(lambda row: ', '.join(row.values.astype(str)), axis=1)
    imported_data_save=pd.concat([buildings_data,gndSubjectCategory['subject_category'],relatedPlaceOrGeographicName['relatedPlaceOrGeographicName'],professionalRelationship['professionalRelationship'], relatedSubjectHeading['relatedSubjectHeading']], axis=1).reset_index()
    imported_data_save=pd.DataFrame(imported_data_save,columns=['dc:identifier','gndo:gndIdentifier','schema:alternateName','gndo:place.gndo:abbreviatedNameForThePlaceOrGeographicName','gndo:placeOrGeographicName','gndo:preferredNameForThePlaceOrGeographicName','gndo:variantNameForThePlaceOrGeographicName','relatedPlaceOrGeographicName', 'gndo:dateOfProduction.schema:date','professionalRelationship','gndo:biographicalOrHistoricalInformation', 'subject_category', 'relatedSubjectHeading', 'gndo:geographicAreaCode','gndo:place.rdfs:literal', 'gndo:place.gndo:gndIdentifier', 'gndo:hierarchicalSuperiorOfPlaceOrGeographicName.rdfs:literal','schema:address.@type', 'schema:address.schema:streetAddress','schema:address.schema:postalCode','schema:geo.@type', 'schema:geo.schema:latitude', 'schema:geo.schema:longitude', 'gndo:broaderTermInstantial.rdfs:literal', 'gndo:broaderTermInstantial.gndo:gndIdentifier']).astype(str)
    imported_data_save =imported_data_save.drop_duplicates(subset=['dc:identifier'],keep='first')
    imported_data_save=imported_data_save.rename(columns={
        'dc:identifier':'dc_identifier',
        'gndo:gndIdentifier':'gnd_identifier',
        'schema:alternateName':'alternate_name',
        'gndo:place.gndo:abbreviatedNameForThePlaceOrGeographicName':'abbreviated_name',
        'gndo:placeOrGeographicName':'place_name',
        'gndo:dateOfProduction.schema:date':'production_date',
        'gndo:preferredNameForThePlaceOrGeographicName':'preferred_name',
        'gndo:variantNameForThePlaceOrGeographicName':'variant_name',
        'relatedPlaceOrGeographicName':'related_place_name', 
        'professionalRelationship':'professional_relationship',
        'gndo:biographicalOrHistoricalInformation':'biographical_information', 
        'gndo:gndSubjectCategory':'subject_category', 
        'relatedSubjectHeading':'related_subject_heading', 
        'gndo:geographicAreaCode':'geographic_area_code',
        'gndo:place.rdfs:literal':'place_literal', 
        'gndo:place.gndo:gndIdentifier':'place_gnd_identifier', 
        'gndo:hierarchicalSuperiorOfPlaceOrGeographicName.rdfs:literal':'hierarchical_place_name_literal',
        'schema:address.@type':'address_type', 
        'schema:address.schema:streetAddress':'street_address',
        'schema:address.schema:postalCode':'postal_code',
        'schema:geo.@type':'geo_type', 
        'schema:geo.schema:latitude':'latitude', 
        'schema:geo.schema:longitude':'longitude', 
        'gndo:broaderTermInstantial.rdfs:literal':'broader_term_instantial_literal', 
        'gndo:broaderTermInstantial.gndo:gndIdentifier':'broader_term_instantial_gnd_identifier'
    })

    st.write(imported_data_save.astype(str))
    csv = convert_df(imported_data_save) 
    st.download_button("Zum Herunterladen drücken", csv, "file.csv", "text/csv")
    return imported_data_save