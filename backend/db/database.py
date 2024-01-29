from sqlalchemy import create_engine, Column, Integer, String, MetaData,DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

SQLALCHEMY_DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class QueriedBuildingDataObject(Base):
    __tablename__ = "queried_private_building_data"
    id = Column(Integer, primary_key=True, index=True)
    dc_identifier = Column(String)
    name = Column(String)
    street_address = Column(String)
    place = Column(String)
    lat_org = Column(String)
    lon_org = Column(String)
    display_name_org = Column(String)
    date_of_query = Column(String)
    default_title = Column(String)
    queried_by = Column(String)
    alternative_title = Column(String)
    comment_content = Column(String)
    public = Column(String)
    index = Column(String)
    target_query_system = Column(String)


class QueriedPrivateDataObject(Base):
    __tablename__ = "queried_private_person_data"
    id = Column(Integer, primary_key=True, index=True)
    date_of_query = Column(String)
    default_title = Column(String)
    alternative_title = Column(String)
    comment_content = Column(String)
    dc_id = Column(String)
    person_id = Column(String)
    data_history = Column(String)
    source_system = Column(String)
    entity_type = Column(String)
    surname = Column(String)
    forename = Column(String)
    personal_name = Column(String)
    name_addition = Column(String)
    counting = Column(String)
    prefix = Column(String)
    non_preferred_name = Column(String)
    preferred = Column(String)
    gender = Column(String)
    birthdate = Column(String)
    deathdate = Column(String)
    birthplace = Column(String)
    deathplace = Column(String)
    profession = Column(String)
    descriptions = Column(String)
    queried_by = Column(String)
    public = Column(String)
    query_id = Column(String)
    url = Column(String)
    total_items = Column(String)
    target_data_source = Column(String)

class PrivateDataWikiresultsObject(Base):
    __tablename__ = "private_person_data_results_wikidata"
    id = Column(Integer, primary_key=True, index=True)
    date_of_query = Column(String)
    default_title = Column(String)
    queried_by = Column(String)
    alternative_title = Column(String)
    comment_content = Column(String)
    public = Column(String)
    dc_id = Column(String)
    person_id = Column(String)
    source_system = Column(String)
    label_value = Column(String)
    item_value = Column(String)
    dob_value = Column(String)
    dod_value = Column(String)
    pob_value = Column(String)
    pod_value = Column(String)
    pobLabel_value = Column(String)
    podLabel_value = Column(String)
    target_data_source=Column(String)

class PrivateDataresultsObject(Base):
    __tablename__ = "private_person_data_results"
    id = Column(Integer, primary_key=True, index=True)
    date_of_query = Column(String)
    default_title = Column(String)
    alternative_title = Column(String)
    comment_content = Column(String)
    source_system= Column(String)
    query_index = Column(String)
    query_id = Column(String)
    totalItems = Column(String)
    url = Column(String)
    nds_id = Column(String)
    dc_id = Column(String)
    person_id = Column(String)
    index = Column(String)
    gnd_id = Column(String)
    gnd_uri = Column(String)
    preferred_name = Column(String)
    variant_name = Column(String)
    date_of_birth = Column(String)
    date_of_death = Column(String)
    place_of_birth = Column(String)
    place_of_death = Column(String)
    profession_or_occupation = Column(String)
    total_score = Column(String)
    max_name_score = Column(String)
    pref_name_score = Column(String)
    var_name_score = Column(String)
    non_pref_name_score = Column(String)
    non_pref_var_name_score = Column(String)
    istmx_x_anz = Column(String)
    istmax_name_score = Column(String)
    max_job_score = Column(String)
    h_deathdate_score = Column(String)
    g_birthdate_score = Column(String)
    o_birthplace_score = Column(String)
    p_deathplace_score = Column(String)
    activity_period_ijklmn_score = Column(String)
    match_status = Column(String)
    comment = Column(String)
    queried_by=Column(String)
    public=Column(String)
    target_data_source=Column(String)

class PrivateBuildingObject(Base):
    __tablename__ = "building_object_private_data"
    id = Column(Integer, primary_key=True, index=True)
    dc_identifier = Column(String)
    gnd_identifier = Column(String)
    alternate_name = Column(String)
    abbreviated_name = Column(String)
    place_name = Column(String)
    production_date = Column(String)
    preferred_name = Column(String)
    variant_name = Column(String)
    related_place_name = Column(String)
    professional_relationship = Column(String)
    biographical_information = Column(String)
    subject_category = Column(String)
    related_subject_heading = Column(String)
    geographic_area_code=Column(String)
    place_literal=Column(String)
    place_gnd_identifier=Column(String)
    hierarchical_place_name_literal=Column(String)
    address_type=Column(String)
    street_address=Column(String)
    postal_code=Column(String)
    geo_type=Column(String)
    latitude=Column(String)
    longitude=Column(String)
    broader_term_instantial_literal=Column(String)
    broader_term_instantial_gnd_identifier=Column(String)
    data_id=Column(String)
    default_title=Column(String)
    date_of_import=Column(String)
    alternative_title=Column(String)
    comment_content=Column(String)
    imported_by=Column(String)
    user_id=Column(String)
    public=Column(String)

class BuildingObject(Base):
    __tablename__ = "private_building_data_results"
    id = Column(Integer, primary_key=True, index=True)
    source_data_id = Column(String)
    date_of_query = Column(String)
    default_title = Column(String)
    alternative_title = Column(String)
    comment_content = Column(String)
    display_name_org = Column(String)
    preferredName = Column(String)
    display_name_target_geofabrik = Column(String)
    lev_distance_geofabrik = Column(String)
    distance_score_geofabrik = Column(String)
    gnd_id_org = Column(String)
    lev_distance_lobid = Column(String)
    distance_score_lobid = Column(String)
    gnd_id_lobid = Column(String)
    gnd_id_score = Column(String)
    geo_distance_score = Column(String)
    dms_lat = Column(String)
    dms_lon = Column(String)
    match_status = Column(String)
    comment = Column(String)
    total_score = Column(String)
    queried_by = Column(String)
    public = Column(String)
    index = Column(String)
    display_name_target_wikidata = Column(String)
    lev_distance_wikidata = Column(String)
    lev_distance_score_wikidata = Column(String)
    display_name_target_wikidata_org = Column(String)
    wikidata_id = Column(String)
    wikidata_coordinates = Column(String)
    target_query_system = Column(String)

class PrivatePersonObject(Base):
    __tablename__ = "person_object_private_data"
    id = Column(Integer, primary_key=True, index=True)
    index=Column(String)
    dc_id=Column(String)
    forename=Column(String)
    surname=Column(String)
    personal_name=Column(String)
    name_addition=Column(String)
    prefix=Column(String)
    counting=Column(String)
    gender=Column(String)
    birth_date=Column(String)
    death_date=Column(String)
    dc_identifier=Column(String)
    non_preferred_name_surname=Column(String)
    birth_place=Column(String)
    death_place=Column(String)
    period_of_activity_start=Column(String)
    period_of_activity_end=Column(String)
    profession=Column(String)
    data_id=Column(String)
    default_title=Column(String)
    date_of_import=Column(String)
    alternative_title=Column(String)
    comment_content=Column(String)
    imported_by=Column(String)
    user_id=Column(String)
    public=Column(String)


class UserBase(BaseModel):
    username: str
    password: str
    name: str
    surname: str
    userrole: str
    email: str
    date:str
    project_role:str
    biography:str
    organization:str

class UserCreate(UserBase):
    pass

class UserUpdate(UserBase):
    last_login: datetime

class User(Base):
    __tablename__ = "user_table"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    name = Column(String)
    surname = Column(String)
    userrole = Column(String)
    email = Column(String, unique=True, index=True)
    password = Column(String)
    date = Column(String)
    last_login = Column(DateTime(timezone=True), default=datetime.utcnow)
    project_role=Column(String)
    biography=Column(String)
    organization=Column(String)

class UserInResponse(BaseModel):
    id: int
    username: str
    name: str
    surname: str
    userrole: str
    email: str
    date: datetime
    last_login: datetime
    project_role:str
    biography:str
    organization:str

class UserInListResponse(UserInResponse):
    pass

class BlogPostDB(Base):
    __tablename__ = "blog_posts"
    id = Column(Integer, primary_key=True, index=True)
    author = Column(String)
    title = Column(String)
    article = Column(String)
    postdate = Column(String)
    email = Column(String)
    language = Column(String)

class BlogPost(BaseModel):
    author: str
    title: str
    article: str
    postdate: str
    email: str
    language: str

Base.metadata.create_all(bind=engine)

def get_building_data():
    db = SessionLocal()
    try:
        query_result = db.query(BuildingObject).all()
        data = [{"source_data_id":item.source_data_id,
                 "date_of_query": item.date_of_query, 
                 "default_title": item.default_title, 
                 "comment_content": item.comment_content, 
                 "display_name_org": item.display_name_org, 
                 "display_name_target_geofabrik":item.display_name_target_geofabrik, 
                 "gnd_id_org":item.gnd_id_org,
                 "preferredName": item.preferredName,
                 "lev_distance_lobid":item.lev_distance_lobid,
                 "gnd_id_lobid":item.gnd_id_lobid,
                 "gnd_id_score":item.gnd_id_score,
                 "geo_distance_score":item.geo_distance_score,
                 "dms_lat":item.dms_lat,
                 "dms_lon":item.dms_lon,
                 "match_status":item.match_status,
                 "comment":item.comment,
                 "distance_score_lobid":item.distance_score_lobid,
                 "total_score": item.total_score} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()

def get_building_private_data():
    db = SessionLocal()
    try:
        query_result = db.query(PrivateBuildingObject).all()
        data = [{"id":item.id,
                "dc_identifier":item.dc_identifier,
                "gnd_identifier":item.gnd_identifier,
                "alternate_name":item.alternate_name,
                "abbreviated_name":item.abbreviated_name,
                "place_name":item.place_name,
                "production_date":item.production_date,
                "preferred_name":item.preferred_name,
                "variant_name":item.variant_name,
                "related_place_name":item.related_place_name,
                "professional_relationship":item.professional_relationship,
                "biographical_information":item.biographical_information,
                "subject_category":item.subject_category,
                "related_subject_heading":item.related_subject_heading,
                "geographic_area_code":item.geographic_area_code,
                "place_literal":item.place_literal,
                "place_gnd_identifier":item.place_gnd_identifier,
                "hierarchical_place_name_literal":item.hierarchical_place_name_literal,
                "address_type":item.address_type,
                "street_address":item.street_address,
                "postal_code":item.postal_code,
                "geo_type":item.geo_type,
                "latitude":item.latitude,
                "longitude":item.longitude,
                "broader_term_instantial_literal":item.broader_term_instantial_literal,
                "broader_term_instantial_gnd_identifier":item.broader_term_instantial_gnd_identifier,
                "data_id":item.data_id,
                "default_title":item.default_title,
                "date_of_import":item.date_of_import,
                "alternative_title":item.alternative_title,
                "comment_content":item.comment_content,
                "imported_by":item.imported_by,
                "user_id":item.user_id,
                "public":item.public} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()


def get_person_private_data():
    db = SessionLocal()
    try:
        query_result = db.query(PrivatePersonObject).all()
        data = [{"id":item.id,
                 "index": item.index, 
                 "dc_id": item.dc_id, 
                 "forename": item.forename, 
                 "surname": item.surname, 
                 "personal_name":item.personal_name, 
                 "name_addition":item.name_addition,
                 "prefix": item.prefix,
                 "counting":item.counting,
                 "gender":item.gender,
                 "birth_date":item.birth_date,
                 "death_date":item.death_date,
                 "dc_identifier":item.dc_identifier,
                 "non_preferred_name_surname":item.non_preferred_name_surname,
                 "birth_place":item.birth_place,
                 "death_place":item.death_place,
                 "period_of_activity_start":item.period_of_activity_start,
                 "period_of_activity_end": item.period_of_activity_end,
                 "profession": item.profession,
                 "data_id":item.data_id,
                 "default_title":item.default_title,
                 "date_of_import":item.date_of_import,
                 "alternative_title":item.alternative_title,
                 "comment_content":item.comment_content,
                 "imported_by":item.imported_by,
                 "user_id":item.user_id,
                 "public":item.public} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()

def get_queried_private_person_data():
    db = SessionLocal()
    try:
        query_result = db.query(QueriedPrivateDataObject).all()
        data = [{"id":item.id,
            "date_of_query":item.date_of_query,
            "default_title":item.default_title,
            "alternative_title":item.alternative_title,
            "comment_content":item.comment_content,
            "dc_id":item.dc_id,
            "person_id":item.person_id,
            "data_history":item.data_history,
            "source_system":item.source_system,
            "entity_type":item.entity_type,
            "surname":item.surname,
            "forename":item.forename,
            "personal_name":item.personal_name,
            "name_addition":item.name_addition,
            "counting":item.counting,
            "prefix":item.prefix,
            "non_preferred_name":item.non_preferred_name,
            "preferred":item.preferred,
            "gender":item.gender,
            "birthdate":item.birthdate,
            "deathdate":item.deathdate,
            "birthplace":item.birthplace,
            "deathplace":item.deathplace,
            "profession":item.profession,
            "descriptions":item.descriptions,
            "queried_by":item.queried_by,
            "public":item.public,
            "query_id":item.query_id,
            "url":item.url,
            "target_data_source":item.target_data_source,
            "total_items":item.total_items} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()

def get_private_person_results():
    db = SessionLocal()
    try:
        query_result = db.query(PrivateDataresultsObject).all()
        data = [{"id":item.id,
            "date_of_query":item.date_of_query,
            "default_title":item.default_title,
            "alternative_title":item.alternative_title,
            "comment_content":item.comment_content,
            "source_system":item.source_system,
            "query_index":item.query_index,
            "query_id":item.query_id,
            "totalItems":item.totalItems,
            "url":item.url,
            "nds_id":item.nds_id,
            "dc_id":item.dc_id,
            "person_id":item.person_id,
            "index":item.index,
            "gnd_id":item.gnd_id,
            "gnd_uri":item.gnd_uri,
            "preferred_name":item.preferred_name,
            "variant_name":item.variant_name,
            "date_of_birth":item.date_of_birth,
            "date_of_death":item.date_of_death,
            "place_of_birth":item.place_of_birth,
            "place_of_death":item.place_of_death,
            "profession_or_occupation":item.profession_or_occupation,
            "total_score":item.total_score,
            "max_name_score":item.max_name_score,
            "pref_name_score":item.pref_name_score,
            "var_name_score":item.var_name_score,
            "non_pref_name_score":item.non_pref_name_score,
            "non_pref_var_name_score":item.non_pref_var_name_score,
            "istmx_x_anz":item.istmx_x_anz,
            "istmax_name_score":item.istmax_name_score,
            "max_job_score":item.max_job_score,
            "h_deathdate_score":item.h_deathdate_score,
            "g_birthdate_score":item.g_birthdate_score,
            "o_birthplace_score":item.o_birthplace_score,
            "p_deathplace_score":item.p_deathplace_score,
            "activity_period_ijklmn_score":item.activity_period_ijklmn_score,
            "match_status":item.match_status,
            "comment":item.comment,
            "queried_by":item.queried_by,
            "target_data_source":item.target_data_source,
            "public":item.public} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()


def get_private_person_wikidata_results():
    db = SessionLocal()
    try:
        query_result = db.query(PrivateDataWikiresultsObject).all()
        data = [{"id":item.id,
            "date_of_query":item.date_of_query,
            "default_title":item.default_title,
            "queried_by":item.queried_by,
            "alternative_title":item.alternative_title,
            "comment_content":item.comment_content,
            "public":item.public,
            "dc_id":item.dc_id,
            "person_id":item.person_id,
            "source_system":item.source_system,
            "label_value":item.label_value,
            "item_value":item.item_value,
            "dob_value":item.dob_value,
            "dod_value":item.dod_value,
            "pob_value":item.pob_value,
            "pod_value":item.pod_value,
            "pobLabel_value":item.pobLabel_value,
            "podLabel_value":item.podLabel_value,
            "target_data_source":item.target_data_source} for item in query_result]
        return pd.DataFrame(data)
    finally:
        db.close()