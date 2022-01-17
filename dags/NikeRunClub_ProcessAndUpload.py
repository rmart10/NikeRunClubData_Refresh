
#######################################################################################################
#################################################### IMPORTS ##########################################


#### AIRFLOW OPERATORS
from email import header
from genericpath import exists
from operator import index
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

### AIRFLOW PROVIDERS
from airflow.providers.http.operators.http import SimpleHttpOperator

### AIRFLOW MODELS AND OTHER
from airflow.models import DAG
from airflow.models import Variable
from airflow import AirflowException


### SENSORS
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.http.sensors.http import HttpSensor


### STANDARD PACKAGES
import sys
import os
from zipfile import ZipFile
import mysql.connector
from mysql.connector import Error
import json
from random import uniform
from datetime import datetime
from numpy.lib.stride_tricks import DummyArray
import pandas as pd

#### complete hack, I can not get airflow to recognize the installed dropbox package.
import imp
stone = imp.load_source('stone', '/opt/airflow/packages/stone/__init__.py')
dropbox = imp.load_source('dropbox', '/opt/airflow/packages/dropbox/__init__.py')


## APPEND TO PATH TO LOAD SCRIPTS DIRECTORY
sys.path.append('/opt/airflow/scripts')

### LOAD CUSTOM SCRIPTS
import NRCParsing


##############################################################################################################
#################################################### INIT VARIABLES ##########################################

## get dropbox token.
token = Variable.get("TOKEN_DROPBOX_API")

## get mysql vars
mysqlDBName = Variable.get("DATABASE_NAME_MYSQL")
mysqlUserName = Variable.get("USER_MYSQL_ADMIN")
mysqlPassword = Variable.get("PW_MYSQL_ADMIN")
mysqlHost = Variable.get("HOST_MYSQL_ADMIN")
## Get Airflow variable that holds mounted dir path to Windows data folder.
dataDirectory = Variable.get("DIR_DATA_LOCAL")  

###relative path to the folder that houses your files on DROPBOX
#(IE OMIT 'Dropbox' from path displayed when viewing folder in UI)
dbxPath = '/Apps/RunGap/export'

## file that is created after processing data, used for importing into mysql
mySQLImportFile = dataDirectory + '/NRCData/for_import.csv'

# #folder relative to this script you want to store retrieved files in.
dataPath = dataDirectory + "/NRCData/"


##############################################################################################################
#################################################### DEFAULT ARGS ############################################

default_args = {
    'start_date': datetime(2020, 1, 1),
    'provide_context': True
}


##############################################################################################################
#################################################### FUNCTIONS ############################################


def delete_import_file():
    '''Function will remove the file associated with DAG variable 'mySQLImportFile''' 
    os.remove(mySQLImportFile)
    
def searchForFile(fileName,searchPath): 
    '''Function will search for a fileName within a searchPath. If found, True is returned else False is returned
    Accepts/Requires 2 parameters: fileName(str), searchPath(str)''' 
    
    found = True
    for root, dir, files in os.walk(searchPath):
        if fileName in files:
            found = True
        else:
            found = False                
    return found

def insertIntoMYSQL():
    '''Function will insert a row into the mysql database using the file path passed in.
    Accepts/Requires 1 parameter: filepath of import file (str)'''   
    
    connection = mysql.connector.connect(host=mysqlHost,
                                    database=mysqlDBName,
                                    user=mysqlUserName,
                                    password=mysqlPassword)
    if connection.is_connected():
        
        db_Info = connection.get_server_info()
        
        ### create the cursor.
        cursor = connection.cursor()       

        ## read the import file into df
        ## file sensor already caught this, but the Airflow webserver will attempt to parse the DAG
        ## prior to run and not see the file, the try avoids the DAG import error.
        try:
            df = pd.read_csv(mySQLImportFile) 
        except:
            print("catching Airflow rendering DAG before running.")           

        # creating column list for insertion
        cols = "`,`".join([str(i) for i in df.columns.tolist()])        

        # Insert DataFrame recrds one by one.
        for i,row in df.iterrows():
            sql = "INSERT INTO `NikeRunClubData` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))

            # the connection is not autocommitted by default, so we must commit to save our changes
            connection.commit()        
        
def no_new_files():
    '''Function simply prints a message in logs indicating the no new files found.
    '''      
    print("No new files found")  

def finish_processing(ti):
    '''Function simply prints a message in logs indicating the number of files processed. This is sourced from teh new_files_list xcom.
    '''  
    try:
        filesToProcess = ti.xcom_pull(key='new_files_list')
        print(len(filesToProcess), " files processed.")       
    except:
        print("")
    
      
        
def downloadNewFiles(ti):
    '''Function will download all new/unfound files available witin a Dropbox folder using variables already initialized (dbxPath,dataPath)
    '''       
    newFileList = []

    dbx = dropbox.Dropbox(token)

    #populate variable(class 'dropbox.files.ListFolderResult) with all files available for download.
    result = dbx.files_list_folder(dbxPath)     
    print("###### Found ", len(result.entries), " files...") 
    
    ##init and set fileFound to True.
    fileFound=True

    ##init and set return control var to False
    newFilesFound=False

    ##iterate each entry found and download to the relative path specified in dataPath
    ##will overwrite existing files.
    for entry in result.entries:
        ##first check if the zip source exists?        
        if str(entry.name).endswith('.zip'):
            fileFound = searchForFile(entry.name,dataPath)
            if not fileFound:
                ##update return control var
                newFilesFound = True
                ##download it
                dbx.files_download_to_file(dataPath+entry.name,entry.path_lower)
                print("Extracting ",entry.name)

                #extract it
                with ZipFile(dataPath + entry.name,'r') as zipObj:
                    try:
                        zipObj.extractall(dataPath) 
                        newFileList.append(dataPath + entry.name)                     
                    except Exception as e:
                        print("Error", e)
                print("###### Done downloading and extracting files...") 

    ## if new files are found....
    ## set the newFilesList object's value to be the xcom new_files_list's value.
    ## return the appropriate branch to the BranchPythonOperator

    if newFilesFound == True:
        ti.xcom_push(key='new_files_list',value=newFileList)
        return ['process_data','finish_processing']
    else:
        return ['no_new_files','finish_processing']

def process_data(ti):
    
    filesToProcess = ti.xcom_pull(key='new_files_list')
    
    ## use counter to determine if we need to add header row to output.
    counter=0
    for file in filesToProcess:        
        df = pd.DataFrame()
        df = NRCParsing.processFile(file)
        if counter==0:
            df.to_csv(mySQLImportFile,mode='a',header=True, index=False)
        else:
            df.to_csv(mySQLImportFile,mode='a',header=False,index=False)
        counter+=1       
        

def _handle_error(**kwargs):
    print("error trapped")    
    
    
with DAG('NikeRunClubData_Refresh', schedule_interval='@daily', default_args=default_args, catchup=False, tags=['NikeRunClub']) as dag:

    is_dropbox_available = HttpSensor(
        task_id='is_dropbox_available',
        http_conn_id='HTTP_DROPBOX',
        endpoint='')

    check_for_new_files = BranchPythonOperator(
        task_id='check_for_new_files',
        python_callable=downloadNewFiles    
        
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

    detect_import_file = FileSensor(
        task_id='detect_import_file',
        poke_interval=30,filepath=mySQLImportFile
    )

    import_new_data = PythonOperator(
        task_id='import_new_data',
        python_callable=insertIntoMYSQL
    )

    delete_import_file = PythonOperator(
        task_id='delete_import_file',
        python_callable=delete_import_file
    )

    no_new_files = PythonOperator(
        task_id='no_new_files',
        python_callable=no_new_files
    )
    
    finish_processing = PythonOperator(
        task_id='finish_processing',
        python_callable=finish_processing,
        trigger_rule='none_failed_or_skipped'    
        
    )      

    is_dropbox_available >> check_for_new_files >> process_data >> detect_import_file >> import_new_data >> delete_import_file >> finish_processing
    is_dropbox_available >> check_for_new_files >> no_new_files >> finish_processing