import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(page_title='GND4C', layout="wide")
st.subheader("Impressum")
components.html(
    """
    <html>
    <head>
        <title></title>
    </head>
    <frameset>
        <frame src='https://www.thulb.uni-jena.de/impressum/?embedded=true'>
    </frameset>
    </html>
        """,
    height=600,
)

