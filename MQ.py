import json
import paho.mqtt.client as mqtt
import time
import datetime
import ast

class myMQ():
    def __init__(self, client, C_info):
        self.client = client
        self.return_dict = {}
        self.flag = False
        self.Info = C_info
        self.disc_flag = True
        self.reconn_flag = False
        self.Topics = []
        
    @property
    def Info(self):
        return self._Info

    @Info.setter
    def Info(self,info):
        self._Info = info

        
    def on_connect(self,client, userdata, flags, rc):
        if rc==0:
            if self.reconn_flag:
                for tpc in self.Topics:
                    self.client.subscribe(tpc)
                self.reconn_flag=False
                print("Connection OK")
                print("Subscribe OK")
            else:
                print("Connection OK")
        else:
            print("Not Connect")     
        print("Connected with result code "+str(rc))

        
    def on_disconnect(self,client, userdata, rc):
        if rc != 0:
            print("Unexpected disconnection.")
        else:
            print("connection disable")
            self.reconnection()
            

    def on_publish(self, mqttc, obj, mid):
        print("Published  mid: "+str(mid))


    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        print("Subscribed mid: "+str(mid)+ " "+ str(granted_qos))


    def on_message(self,client, userdata, msg):
        self.return_dict = msg.payload.decode("utf-8")  # class bytes to string
        self.return_dict = ast.literal_eval(self.return_dict)
        self.flag = True


    def reconnection(self):
        if self.disc_flag:
            self.client.reconnect()
            self.reconn_flag = True
        else:
            pass
    
    def Get_Message(self):
        if self.flag == True:
            self.flag = False
            return self.return_dict
        else:
            return False
            

    def setCLA(self):
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.connect(self._Info[0],  self._Info[1], 60)
    
        
    def subscribeTopics(self,*topics):
        for topic in topics:
            self.client.subscribe(topic)
            if topic not in self.Topics:
                self.Topics.append(topic)
            else:
                pass


    def publishDataString(self,publishTopic,DataString):
        self.client.publish(publishTopic,DataString)


    def startRun(self):
        self.client.loop_start()


    def stopLoop(self):
        self.disc_flag = False
        self.client.disconnect()
        self.client.loop_stop()
