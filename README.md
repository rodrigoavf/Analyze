# Analyze
Analyze is a complete system of economic, financial, accounting, and managerial analysis for companies of all sectors and sizes that have updated and reliable accounting.

# How it was built
It was originally built in Power BI and Excel, each one having it own version. Later I decided to try to rebuild it using Python for processing the data in a much faster way then Excel, and Streamlit to replace Power BY dashboard.

# .py files
## Analyze.py
This is the main file that runs th Streamlit app.

## Tratamento.py
This is were the data get calculated, once the user uploads the necessary files to the app all data is calculated in here and displyed by analyze.py

## csv_delimiter.py
This is just a funciont to guaranteee that the uploaded csv files will be in the expected format.

# .csv files
There are a total of 7 .csv files and they serve as structure models of the data.
