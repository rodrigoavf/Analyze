# Analyze
Analyze is a complete system of economic, financial, accounting, and managerial analysis for companies of all sectors and sizes that have updated and reliable accounting.

# How it was built
It was originally built in Power BI and Excel, each one having its own version. Later I decided to try to rebuild it using Python for processing the data in a much faster way than Excel, and used Streamlit to replace Power BI dashboard.

# Analyze.py
This is the main file that runs th Streamlit app.

# tratamento.py
This is were the data get calculated, once the user uploads the necessary files to the app all data is calculated in here and displyed by analyze.py

# .csv files
There are a total of 7 .csv files and they serve as structure models of the data.
