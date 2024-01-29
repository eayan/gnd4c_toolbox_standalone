import streamlit as st
import pandas as pd
import datetime
import requests
from datetime import datetime


BACKEND_URL = "http://localhost:8000/admin"

def user_registration():
    username = st.text_input("Username*", key="username")
    password = st.text_input("Password*", type="password", key="password")
    name = st.text_input("Name*")
    surname = st.text_input("Surname*")
    userrole= st.radio("User Role*", options=["Admin", "Superuser", "User"], horizontal=True, label_visibility="visible")
    email = st.text_input("Email*")
    date=st.date_input("Day of Registration")
    project_role = st.text_input("Project Role*")
    organization= st.text_input("Organization*")
    biography= st.text_area("Biography", value=" ")

    if username and len(username) > 0 and password and len(password) > 0 and name and len(name) > 0 and surname and len(surname) > 0 and len(userrole) > 0 and len(email) > 0 and len(project_role) > 0 and len(organization) > 0 and st.button("Register"):
        response = requests.post(
            "http://localhost:8000/register",
            data={"username": username, "password": password, "name": name, "surname": surname, "userrole":userrole, "email": email, "date":date, "project_role":project_role,"organization":organization,"biography":biography}
        )
        if response.status_code == 200:
            st.success("User registered successfully")
        else:
            st.error("Error registering user")
    else:
        st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")

def list_users():
    st.subheader("User List")
    BACKEND_URL = "http://localhost:8000"
    response = requests.get(f"{BACKEND_URL}/get_users/")
    if response.status_code == 200:
        users = response.json()
        st.table(users)
    else:
        st.error("Error fetching user list.")

def get_user_by_username(username):
    BACKEND_URL = "http://localhost:8000"
    response = requests.get(f"{BACKEND_URL}/users/{username}")
    if response.status_code == 200:
        user = response.json()
        user = pd.DataFrame(user, columns=['username','name', 'surname','email', 'userrole','project_role', 'organization', 'biography']).transpose()
        st.table(user)
    else:
        st.error("Error fetching user list.")

def update_user():
    st.subheader("Update User")
    user_id = st.text_input("User ID:", key="user_id")
    
    # Fetch existing user data if user ID is provided
    existing_user_data = None
    if user_id:
        response = requests.get(f"{BACKEND_URL}/users/{user_id}")
        if response.status_code == 200:
            existing_user_data = response.json()
    
    # Display the form with existing user data as default values
    user_data = {
        "username": st.text_input("Username:", value=existing_user_data["username"] if existing_user_data else ""),
        "password": st.text_input("Password:", type="password", value=""),
        "name": st.text_input("Name:", value=existing_user_data["name"] if existing_user_data else ""),
        "surname": st.text_input("Surname:", value=existing_user_data["surname"] if existing_user_data else ""),
        "userrole": st.radio("User Role", options=["Admin", "Superuser", "User"], horizontal=True, label_visibility="visible", key="user_update"),
        "email": st.text_input("Email:", value=existing_user_data["email"] if existing_user_data else ""),
        "date": str(st.date_input("Day of Update")),
        "last_login":datetime.utcnow().isoformat()
    }
    
    if st.button("Update"):
        response = requests.put(f"{BACKEND_URL}/users/{user_id}", json=user_data)
        if response.status_code == 200:
            st.success("User updated successfully.")
        else:
            st.error(f"Error updating user. Status code: {response.status_code}, Message: {response.text}")
def update_single_user(user_id, user_role):
    user_id = user_id
    
    # Fetch existing user data if user ID is provided
    existing_user_data = None
    if user_id:
        response = requests.get(f"{BACKEND_URL}/users/{user_id}")
        if response.status_code == 200:
            existing_user_data = response.json()
    
    # Display the form with existing user data as default values
    user_data = {
        "username": st.text_input("My Username:", value=existing_user_data["username"] if existing_user_data else ""),
        "password": st.text_input("My Password:", type="password", value=""),
        "name": st.text_input("My Name:", value=existing_user_data["name"] if existing_user_data else ""),
        "surname": st.text_input("My Surname:", value=existing_user_data["surname"] if existing_user_data else ""),
        "userrole": user_role,
        "email": st.text_input("My Email:", value=existing_user_data["email"] if existing_user_data else ""),
        "date": str(st.date_input("My Day of Update")),
        "project_role": st.text_input("My Project Role:", value=existing_user_data["project_role"] if existing_user_data else ""),
        "organization": st.text_input("My Organization:", value=existing_user_data["organization"] if existing_user_data else ""),
        "biography": st.text_area("My Biography:", value=existing_user_data["biography"] if existing_user_data else ""),
        "last_login":datetime.utcnow().isoformat()
    }
    
    if st.button("Update My Data"):
        response = requests.put(f"{BACKEND_URL}/users/{user_id}", json=user_data)
        if response.status_code == 200:
            st.success("User updated successfully.")
        else:
            st.error(f"Error updating user. Status code: {response.status_code}, Message: {response.text}")

def delete_user():
    st.subheader("Delete User")
    user_id = st.text_input("User ID:")
    
    # Fetch existing user data if user ID is provided
    existing_user_data = None
    if user_id:
        response = requests.get(f"{BACKEND_URL}/users/{user_id}")
        if response.status_code == 200:
            existing_user_data = response.json()
    
    # Display the confirmation message with user data
    if existing_user_data:
        st.write(f"Are you sure you want to delete the following user?\n\n"
                 f"**Username:** {existing_user_data['username']}\n"
                 f"**Name:** {existing_user_data['name']}\n"
                 f"**Surname:** {existing_user_data['surname']}\n"
                 f"**UserRole:** {existing_user_data['userrole']}\n"
                 f"**Email:** {existing_user_data['email']}\n")
        
        if st.button("Delete"):
            response = requests.delete(f"{BACKEND_URL}/users/{user_id}")
            if response.status_code == 200:
                st.success("User deleted successfully.")
            else:
                st.error(f"Error deleting user. Status code: {response.status_code}, Message: {response.text}")
    else:
        st.warning("Please enter a valid User ID to delete a user.")