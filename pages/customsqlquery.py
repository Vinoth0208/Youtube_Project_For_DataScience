import mysql.connector
import pandas as pd
import streamlit

query=streamlit.text_area("Enter the query to execute:",height=100)
state=streamlit.button("execute")
if state:
    myConnection = mysql.connector.connect(host="localhost", user="root", password="root")
    mycursor = myConnection.cursor()
    mycursor.execute("use youtube;")
    mycursor.execute(query)
    rs=mycursor.fetchall()
    # streamlit.write(rs)
    df=pd.DataFrame(rs)
    streamlit.write(df)

