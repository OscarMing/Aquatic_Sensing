from MQ import myMQ
import pygsheets
import signal
import paho.mqtt.client as mqtt
import json
import time
import create_table as ct
from datetime import datetime

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Broker_IP = "XXX.XXX.OOO.OOO"
Broker_PORT = 0000

DB_IP = "XXX.XXX.OOO.OOO"
DB_PORT = 0000
DB_USER = "XXX"
DB_PWD = "OOO"
DB_SCHEMA = "YYY"

FLAG = True
client = mqtt.Client()
mqttc = myMQ(client,(Broker_IP,Broker_PORT))
mqttc.Info = (Broker_IP,Broker_PORT)
mqttc.setCLA()
mqttc.startRun()
topic = '/THE/TOPIC/YOU/DEFINE'
mqttc.subscribeTopics(topic)
Data = {}



def exits(signum, frame):
    global mqttc
    global FLAG
    mqttc.stopLoop()
    FLAG = False
    print('Process Stopping ......')

def paser_msg(MSG):
    global mqttc
    global Data
    #MSG = mqttc.Get_Message()
    if MSG!=False:
        TT = time.localtime( int(MSG['gen_date']))
        Data['Date'] = time.strftime("%Y/%m/%d, %H:%M:%S", TT)
        Data['Chip_ID'] = MSG['chip_id']
        Data['DATA'] = {}
        for key in MSG['raw_data'].keys():
            Data['DATA'][key] = {'values':MSG['raw_data'][key],'status':MSG['op_status'][key]}
        print(Data)

        if 'op90' in MSG['raw_data'].keys():
            #save_gsheet(Data,'S')
            save_db(Data,'S')
        else:
            #save_gsheet(Data,'L')
            save_db(Data,'L')
        
def save_db(data,flag):
    global DB_IP
    global DB_PORT
    global DB_USER
    global DB_PWD
    global DB_SCHEMA


    print(time.ctime())
    print(flag)
    print(data)
    
    CID = data['Chip_ID']
    if flag == 'L':
        O1 = json.dumps(data['DATA']['op1'])
        O2 = json.dumps(data['DATA']['op2'])
        O3 = json.dumps(data['DATA']['op3'])
        O4 = json.dumps(data['DATA']['op4'])
        O5 = json.dumps(data['DATA']['op5'])
        O89 = json.dumps(data['DATA']['op89'])
    elif flag == 'S':
        O90 = json.dumps(data['DATA']['op90'])
        
    T =  data['Date']


    Base = declarative_base()
    
    ci = ct.connection_info(DB_USER,DB_PWD,DB_IP,DB_PORT,DB_SCHEMA,Base)
    eg =ci.createengin()
    try:
        Session = sessionmaker(bind=eg)
        session = Session()
    except Exception as e:
        #Save_Err_Log(Target='SQL', Status=2,)
        print(e)
        
    if flag == 'L':
        obj = ct.WSDATA(chip_id = CID, OP1 = O1,OP2 = O2,OP3 = O3,OP4 = O4,OP5 = O5,OP89=O89,time = T)
    elif flag == 'S':
        obj = ct.WSDATA(chip_id = CID, OP90=O90,time = T)
    session.add(obj)

    try:
         session.commit()
    except Exception as e:
        session.rollback()
        #Save_Err_Log(Target='SQL', Status=3,)
        print(e)
    # save_gsheet(data,flag)


while True:
    signal.signal(signal.SIGINT,exits)
    signal.signal(signal.SIGTERM,exits)
    MSG = mqttc.Get_Message()
    if MSG!=False:
        print(MSG)
        paser_msg(MSG)
        print(type(MSG))
    else:
        time.sleep(60)
        mqttc.subscribeTopics(topic)
        print(MSG)
        paser_msg(MSG)
        print(type(MSG))
    
