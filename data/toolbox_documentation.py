import streamlit.components.v1 as components
import streamlit as st



def download_installation_document():
    with open("documents/app_installation.pdf", "rb") as pdf_file:
        import_document = pdf_file.read()
    st.download_button(label="App Installation Document",
                        data=import_document,
                        file_name="app_installation.pdf",
                        mime='application/octet-stream')


def download_import_document():
    with open("documents/data_import.pdf", "rb") as pdf_file:
        import_document = pdf_file.read()
    st.download_button(label="Data Import Document",
                        data=import_document,
                        file_name="data_import.pdf",
                        mime='application/octet-stream')

def download_document():
    with open("documents/person_data.pdf", "rb") as pdf_file:
        scoring_document = pdf_file.read()
    st.download_button(label="Toolbox Document", data=scoring_document, file_name="toolbox_document.pdf", mime='application/octet-stream')

#this is for dataframe listing
def list_data():
    components.html("""<iframe src="https://miro.com/app/live-embed/uXjVMRE-BzI=/?moveToViewport=-2591,-1380,4866,2511&embedId=914057898960" scrolling="no" allow="fullscreen; clipboard-read; clipboard-write" allowfullscreen width="1400" height="750" frameborder="0"></iframe>""", width=1500, height=750)
