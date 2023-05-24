import json
import requests
import pandas as pd
import time
import numpy as np
import os
import re
import datetime
from requests import Request,Session
from requests.exceptions import ConnectionError,Timeout,TooManyRedirects
from dingtalkchatbot.chatbot import DingtalkChatbot

webhook = 'https://oapi.dingtalk.com/robot/send?access_token=a9789d2739819eab19b07dcefe30df3fcfd9f815bf198ced54c71c557f09e7d9'
session = Session()

addresses = ['0x111cff45948819988857bbf1966a0399e0d1141e']
types = [1]

while True:
    tot_values = []
    for i in range(1):
        sub_address = addresses[i]
        sub_type = types[i]
        # 地址余额是否有变动 监控的地址
        url_2 = 'https://services.tokenview.io/vipapi/addr/b/eth/' + sub_address + '?apikey=5u0dNQPd55eoEwFPwF2A'
        logo = 0
        while logo == 0:
            try:
                response = session.get(url_2)
                data = json.loads(response.text)
                code = str(data['code'])
                if code == '1':
                    value = round(float(data['data']),2)
                    tot_values.append(value)
                    # 读取上一时刻的余额数据
                    name = 'pre_data_' + str(i+1) + '.csv' 
                    pre_data = pd.read_csv(name)
                    pre_data['date'] = pd.to_datetime(pre_data['date'])
                    pre_data = pre_data.sort_values(by='date')
                    pre_data = pre_data.reset_index(drop=True)
                    #print('余额变化——————' + str(value - pre_data['value'][len(pre_data)-1]))
                    #有btc转出时，余额变少了
                    if value - pre_data['value'][len(pre_data)-1] < -100:
                        alert = '【聪明地址转出】：' + str(value - pre_data['value'][len(pre_data)-1]) + '个ETH'
                        logo = 1
                    #有btc转入时，余额变多了    
                    elif value - pre_data['value'][len(pre_data)-1] > 100:
                        alert = '【聪明地址转入】：' + str(value - pre_data['value'][len(pre_data)-1]) + '个ETH'
                        logo = 1
                    #防止粉尘攻击
                    else:
                        logo = 1
                    # 钉钉报警
                    xiaoding = DingtalkChatbot(webhook)
                    xiaoding.send_markdown(title='数据监控', text=alert)
                    #把最新数据写入csv文件中
                    date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df_now = pd.DataFrame({'date':date_now,'address':sub_address,'value':value},index=[0]) 
                    df_now = pd.concat([pre_data,df_now])
                    df_now['date'] = pd.to_datetime(df_now['date'])
                    df_now = df_now.sort_values(by='date')
                    df_now = df_now.reset_index(drop=True)
                    df_now = df_now[-10:]
                    #print(df_now)
                    df_now.to_csv(name,index=False)

                else:
                    logo = 1

            except(ConnectionError,Timeout,TooManyRedirects) as e:
                logo = 0