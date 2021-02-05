from sqlalchemy.sql import text
from sqlalchemy_views import CreateView, DropView
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from datetime import datetime
from datetime import timedelta
import time

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

import os
import csv
import create_table as ct

import schedule


DB_IP = "XXX.XXX.OOO.OOO"
DB_PORT = 0000
DB_USER = "XXX"
DB_PWD = "OOO"
DB_SCHEMA = "YYY"

Base = declarative_base()
ci = ct.connection_info(DB_USER,DB_PWD,DB_IP,DB_PORT,DB_SCHEMA,Base)
eg =ci.createengin()

File_Name = []
retry_count = 5

def Query_Daily_Data():
    global eg
    global File_Name
    global retry_count
    retry_flag = False
    title_A_RAW = ['chip_id','OP90','TIMESTAMP']
    title_A_CONV= ['chip_id','Ultrasonic','TIMESTAMP']
    title_B_RAW = ['chip_id','OP1','OP2','OP3','OP4','OP5','OP89','TIMESTAMP']
    title_B_CONV= ['chip_id','Temperature','TDS','PH','ORP','DO','OP89','TIMESTAMP']
    title = [title_A_RAW,title_A_CONV,title_B_RAW,title_B_CONV]
    filename =  str((datetime.now()- timedelta(days=1)).strftime("%Y_%m_%d"))
    
    File_Name =[f'{filename}_S_A_R',f'{filename}_S_A_C',f'{filename}_S_B_R',f'{filename}_S_B_C']
    STMT = ['sensor1_raw','sensor1_convert','sensor2_raw','sensor2_convert']
    for i in range(len(STMT)):
        if os.path.isfile(f'./Data/{File_Name[i]}.csv'):
            print(f'./Data/{File_Name[i]}.csv already EXIST!!')
            pass
        else:
            stmt = text(f"SELECT * FROM {STMT[i]}  WHERE updatetime  BETWEEN CURDATE() - 1  AND CURDATE()")
            try:
                result = eg.execute(stmt).fetchall()
                with open(f'./Data/{File_Name[i]}.csv', mode='a') as csv_file:
                     writer = csv.writer(csv_file, delimiter=',')
                     writer.writerow(title[i])
                     for j in  range(len(result)):
                         writer.writerow(result[j])
            except Exception as e:
                print(e)
                retry_flag =True


    if retry_flag:
        time.sleep(60)
        Query_Daily_Data()
    else:    
        Upload_To_GDrive()
                     
def Upload_To_GDrive():
    global File_Name
    gauth = GoogleAuth()
    
    gauth.LoadCredentialsFile("mycreds.txt")
    if gauth.credentials is None:

        gauth.GetFlow()
        gauth.flow.params.update({'access_type': 'offline'})
        gauth.flow.params.update({'approval_prompt': 'force'})
        
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        try:
            gauth.Refresh()
        except Exception as e:
            print(e)
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile("mycreds.txt")
    drive = GoogleDrive(gauth)

    folder_id = ['Folder_String_A',
                 'Folder_String_B',
                 'Folder_String_C',
                 'Folder_String_D']
   

    for k in range(len(File_Name)):
        try:
    
            f = drive.CreateFile({'title':f'{File_Name[k]}.csv',"parents": [{"kind": "drive#fileLink", "id": folder_id[k]}]})
            f.SetContentFile(f'./Data/{File_Name[k]}.csv')
            f.Upload()
            print ("Uploading succeeded!")
        except:
            print( "Uploading failed.")
            
    print(time.ctime())


def job():
    print("I'm working...",time.ctime())



schedule.every(6).hours.do(job)
schedule.every().day.at("15:00").do(Query_Daily_Data)
#schedule.every(2).hours.do(Query_Daily_Data)

while True:
    schedule.run_pending()
    time.sleep(60)

