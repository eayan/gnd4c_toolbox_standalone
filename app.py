import streamlit as st
from data.dashboard import fn_visuals 


st.set_page_config(page_title='GND4C', layout="wide")
st.subheader("Home")
st.write("Die GND4C-Toolbox ist ein im Rahmen des Projektes GND4C ([Über GND4C](https://gnd4c.thulb.uni-jena.de/%C3%9Cber_GND4C)) entwickeltes Werkzeug. Zentrales Ziel ist das möglichst eindeutige Matching eigener Datensätze auf Entitäten in der GND sowie davon ausgehend die Identifikation geeigneter Kandidaten aus nichtbibliothekarischen Datenquellen für eine Einspielung in die GND. Der Entwicklungsschwerpunkt liegt bisher auf den auch in der Museums- und Sammlungsdokumentation zentralen Entität Personen, an Erweiterungen für Bauwerke, Geografika und Sachbegriffe wird weiter gearbeitet.Die Toolbox wird in der Entwicklungsphase bis Mitte 2024 von der GND-Pilotagentur [data4kulthura – Servicestelle für Datenqualität und Normdaten](https://dksm.thulb.uni-jena.de/data4kulthura/) an der Thüringer Universitäts- und Landesbibliothek betrieben. Als Python-Anwendung kann die Toolbox nach Projektende verhältnismäßig leicht auch von anderen GND-Agenturen gehostet und weiterentwickelt werden. Der Quellcode wird dafür unter einer offenen Lizenz publiziert. Logins für die Toolbox-Nutzung werden derzeit an Agenturpartner und Testuser aus ähnlichen Projektzusammenhängen vergeben.")

fn_visuals()
st.write("LAST UPDATE: 29.01.2024 By Erdal Ayan")