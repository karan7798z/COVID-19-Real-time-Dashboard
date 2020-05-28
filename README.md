# COVID-19-Real-time-Dashboard

Repository for Real time COVID-19 Dashboard



This Dashboard is created using Tableau Public, and the dataset used is provided by John Hopkins University. I have used a modified version of this dataset, curated [here](https://github.com/imdevskp/covid_19_jhu_data_web_scrap_and_cleaning)

The project was implemented in 3 phases - 
1. Data Acquisition from Github
2. Data Transformation in Python
3. Data Loading in Google Sheets using Google Sheets API
3. Data Visualization in Tableau Public, using real time data from Google Sheets

## Data Acquisition from Github
Data is acquired from [here](https://github.com/imdevskp/covid_19_jhu_data_web_scrap_and_cleaning).

The file - covid.py contains the code to scrape the csv files from this URL. 

The 2 csv files downloaded from this source, are stored in 2 separate dataframes.

Reason why I have downloaded 2 different csv's is because the first csv contains the Country and State-wise distribution of cases for all major countries, except for the United States, and the second csv contains the state-wise as well as the province-wise distribution of cases for the US.

## Data Transformation in Python
Initially, the unused columns are removed from the usa_county_wise.csv, and then the columns are grouped on Country, State and Date, to get the state-wise case count of confirmed and death cases for US.

To create a combined source for state-wise cases for as many countries for which data is available, I have merged the first dataframe (containing data from the csv - covid_19_clean_complete.csv), and the new grouped dataframe for US States.

Thus now, our 2 final dataframes are ready - 
1. Dataframe containing original file data from - covid_19_clean_complete.csv
2. Dataframe containing merged values from USA State Grouped, and covid_19_clean_complete.csv

## Data Loading in Google Sheets using Google Sheets API
Since Tableau has an option of loading datasets from a Google Sheets Data Source, and allows for real time data refresh using this source, I have uploaded the transformed data to Google Sheets from Python, using the `pygsheets` library and Google Sheets API.

I first created a Project in [Google API Console](https://console.developers.google.com/) and enable the Google Sheets and Google Drive API access for this project.

To access the Google Sheets API from our local Python console, we need to authorize pyghseets for this access.

This requires a credential to be created for our Google Console Project. Here's how you can create an appropriate credential:

1. Select ‘Credentials’ (under ‘Library’) from the left pane. This will display you the ‘Credentials’ pane on the right
2. Click on the ‘CREATE CREDENTIALS’ option on the top, which will display a drop-down menu
3. Click on ‘Help me choose’ option from the drop-down menu, which will redirect you to a pane with heading ‘Add credentials to your project’
4. On this ‘Add credentials to your project’ pane follow along:

5. Step 1: ‘Find out what kind of credentials you need’
6. Which API are you using? — Google Sheets API
7. Where will you be calling the API from? — Web server
8. What data will you be accessing? — Application data
9. Are you planning to use this API with App Engine or Compute Engine? — No, I’m not using them
Now, click on ‘What credentials do I need?’ for suggestion as per our input

10. Step 2: ‘Create a service account’
11. Service account name — (for e.g: covid-data-update)
12. Role — Editor (From drop-down select ‘Project’ and then ‘Editor’)
13. Service account ID — auto-created
14. Key type — JSON
15. Now, click on ‘Continue’.
16. This will ask you to save credentials.json file which allows access to your cloud resources — so store it securely.

Now from this credentials file, I copied the email address value from ‘client_email’ key, and pasted this in the 'Share' options of a new Google Sheet, so that I can share that sheet (target sheet) with the client email. This email needs to be given edit access to the sheet.

Next I authorized `pygsheets` to make edits in the created Google Sheet with the below command - 

`client = pygsheets.authorize(service_file='/Users/karan7798z/Desktop/Data_Science/credentials.json')`

and the rest of the code in the covid.py file, ensures that the data is loaded in the Google Sheet upon execution.

## Data Visualization in Tableau Public, using real time data from Google Sheets
Now in Tableau Public, a Google Sheets Data Source is selected and after allowing it access to the Google account where the Google Sheet is loaded with data, it is given the path of our new Google Sheet.

I have attached my Tableau Workbook here for reference.
