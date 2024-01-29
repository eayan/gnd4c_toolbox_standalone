import streamlit as st
import pandas as pd
from datetime import datetime
import requests

def add_blog_post():
    st.subheader("Blog Post Entry")

    author = st.text_input("Geben Sie Ihren Vor- und Nachnamen ein*",max_chars=55)
    title = st.text_input("Geben Sie Ihren Posttitel ein *")
    article = st.text_area("Geben Sie hier Ihren Post ein *", height=50)
    postdate = st.date_input("Post Date", datetime.today())
    postdate =str(postdate)
    email = st.text_input("Geben sie ihre E-Mail Adresse ein*",max_chars=55)
    language = st.selectbox('In welcher Sprache möchten Sie beitragen?',('Deutsch', 'English'))
    if not author:
        st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")
    elif not email:
        st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")
    elif not title:
        st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")
    elif not article:
        st.warning("Bitte füllen Sie die (*) Pflichtfelder aus.")

    if st.button("Submit"):
        blog_post = {
            "author": author,
            "title": title,
            "article": article,
            "postdate": postdate,
            "email": email,
            "language": language
        }

        response = requests.post("http://127.0.0.1:8000/blog/post/", json=blog_post)
        if response.status_code == 200:
            st.success("Blog post created successfully")
        else:
            st.error("Failed to create blog post")

def list_blog_posts():
    st.subheader("Blogpost Ansehen")
    list_blogposts = view_all_blogposts()
    blogpostlist= pd.DataFrame(list_blogposts, columns=["Author", "Post Title", "Post", "Postdate", "Email", "Sprache"])
    st.dataframe(blogpostlist)