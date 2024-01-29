import streamlit as st
import pandas as pd


def fn_dashboard():
    data= pd.read_csv('data/source_list.csv', index_col=0)

def fn_visuals():
    col1, col2, = st.columns([0.4,0.4])
    with col1:
        st.info("Entität: Person")
        with st.expander("Wählen Sie einen Personendatensatz"):
            st.write("Dieser Teil ist im Aufbau!")

    with col2:
        st.info("Entität: Bauwerk")
        with st.expander("Wählen Sie einen Bauwerkdatensatz"):
            st.write("Dieser Teil ist im Aufbau!")