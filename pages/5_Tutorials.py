import streamlit as st
from streamlit_player import st_player

st.set_page_config(page_title='GND4C', layout="wide")
st.subheader("Tutorials")
st.write("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam consequat tortor quis lorem volutpat, venenatis finibus tortor luctus. Mauris nec velit et purus tristique vestibulum nec a turpis. Cras libero odio, vehicula non dictum tristique, dapibus ac mi. Quisque convallis dolor vitae ipsum egestas placerat. Aenean pellentesque sit amet mauris sed blandit. Phasellus tellus sapien, pellentesque sit amet mattis et, molestie sit amet arcu. Ut rutrum finibus ligula, facilisis pharetra augue eleifend bibendum. Quisque eget neque volutpat, eleifend libero vitae, dictum neque. Cras blandit pulvinar nulla, at faucibus lectus hendrerit ac. Vivamus nisl dolor, tristique ullamcorper lacinia vitae, feugiat at ante. Sed venenatis ante et velit ultricies, at tincidunt dolor facilisis. Morbi condimentum sagittis placerat. Sed eu pharetra quam.")
st_player("https://youtu.be/6zb_eIOUJ-M")