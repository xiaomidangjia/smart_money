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
from qiniu import Auth, put_file, etag

def gmt_img_url(key=None,local_file=None,**kwargs):
    # refer:https://developer.qiniu.com/kodo/sdk/1242/python
    # key:上传后保存的文件名；
    # local_file:本地图片路径，fullpath
    # 遗留问题：如果服务器图片已存在，需要对保存名进行重命名

    #需要填写你的 Access Key 和 Secret Key
    access_key = 'svjFs68isTvptqveLl9xBADP9v8s0jZdUzoGe0-U'
    secret_key = 'XRqt6RgoeK9-hZmKyPjPuFQkeYcU0cPNVgKWEl7l'

    #构建鉴权对象
    q = Auth(access_key, secret_key)

    #要上传的空间
    bucket_name = 'carsonlee'

    #生成上传 Token，可以指定过期时间等
    token = q.upload_token(bucket_name, key)

    #要上传文件的本地路径
    ret, info = put_file(token, key, local_file)

    base_url = 'http://ruusug320.hn-bkt.clouddn.com'    #七牛测试url
    url = base_url + '/' + key
    #private_url = q.private_download_url(url)

    return url
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=a9789d2739819eab19b07dcefe30df3fcfd9f815bf198ced54c71c557f09e7d9'
session = Session()

addresses = ['0x111cff45948819988857bbf1966a0399e0d1141e']
types = [1]

while True:
    i = 0
    sub_address = addresses[i]
    sub_type = types[i]
    # 地址余额是否有变动 监控的地址
    url_2 = 'https://services.tokenview.io/vipapi/addr/b/eth/' + sub_address + '?apikey=5u0dNQPd55eoEwFPwF2A'
    logo = 0
    while logo == 0:
        try:
            response = session.get(url_2)
            data = json.loads(response.text)
            print(data)
            code = str(data['code'])
            if code == '1':
                now_value = round(float(data['data']),2)
                # 读取上一时刻的余额数据
                pre_data = pd.read_csv('pre_data_1.csv')
                pre_data['date'] = pd.to_datetime(pre_data['date'])
                pre_data = pre_data.sort_values(by='date')
                pre_data = pre_data.reset_index(drop=True)
                print(pre_data)
                change =  now_value - pre_data['value'][len(pre_data)-1] 
                #有btc转出时，余额变少了
                if change < -100:
                    # 做图片
                    url ='https://services.tokenview.io/vipapi/address/balancetrend/eth/0x111cff45948819988857bbf1966a0399e0d1141e?apikey=5u0dNQPd55eoEwFPwF2A'
                    session = Session()
                    #url= 'https://services.tokenview.io/vipapi/pending/btc/2e0ec4a6caec1cf2f9cd9e58a5a3954c531d20c08bb88ae8d1a91dc0547f3561?apikey=5u0dNQPd55eoEwFPwF2A'
                    #session.headers.update(headers)
                    try:
                        response = session.get(url)
                        data = json.loads(response.text)
                    except(ConnectionError,Timeout,TooManyRedirects) as e:
                        print(e)
                    date = []
                    ba = []
                    for i in range(len(data['data'])):
                        ins = data['data'][i]
                        date.append(next(iter(ins.keys())))
                        ba.append(float(next(iter(ins.values()))))
                    sub_df = pd.DataFrame({'date':date,'value':ba})
                    url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
                    url_name = ['k_fold']
                    # insert your API key here
                    API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
                    data_list = []
                    for num in range(len(url_name)):
                        print(num)
                        addr = url_address[num]
                        name = url_name[num]
                        # make API request
                        res_addr = requests.get(addr,params={'a': 'ETH', 'api_key': API_KEY})
                        # convert to pandas dataframe
                        ins = pd.read_json(res_addr.text, convert_dates=['t'])
                        #ins.to_csv('test.csv')
                        #print(ins['o'])
                        ins['date'] =  ins['t']
                        ins['value'] =  ins['o']
                        ins = ins[['date','value']]
                        data_list.append(ins)
                    result_data = data_list[0][['date']]
                    for i in range(len(data_list)):
                        df = data_list[i]
                        result_data = result_data.merge(df,how='left',on='date')
                    #last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
                    last_data = result_data[(result_data.date>='2013-01-01')]
                    last_data = last_data.sort_values(by=['date'])
                    last_data = last_data.reset_index(drop=True)
                    print(type(last_data))
                    date = []
                    open_p = []
                    close_p = []
                    high_p = []
                    low_p = []
                    for i in range(len(last_data)):
                        date.append(last_data['date'][i])
                        open_p.append(last_data['value'][i]['o'])
                        close_p.append(last_data['value'][i]['c'])
                        high_p.append(last_data['value'][i]['h'])
                        low_p.append(last_data['value'][i]['l'])
                    #res_data = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})
                    res_data = pd.DataFrame({'date':date,'close':close_p})
                    res_data = res_data.sort_values(by=['date'])
                    res_data = res_data.reset_index(drop=True)
                    sub_df['date'] = pd.to_datetime(sub_df['date'])
                    res_df = sub_df.merge(res_data[['date','close']],how='left',on=['date'])
                    res_df = res_df.sort_values(by='date')
                    import matplotlib.pyplot as plt
                    import seaborn as sns
                    # 绘画折线图
                    f, axes = plt.subplots(figsize=(20, 10))
                    axes_fu = axes.twinx()
                    sns.lineplot(x="date", y="value",color='red', linewidth=0.5,data=res_df, ax=axes)
                    sns.lineplot(x="date", y="close", data=res_df, ax=axes_fu)
                    plt.title('SMART ADDRESS', fontsize=20)

                    title_asset_pool = str(datetime.datetime.now().strftime('%Y-%m-%d'))
                    fig_name = str(title_asset_pool) + '_smart_address' + '.png'
                    plt.savefig(fig_name,bbox_inches='tight')
                    plt.close()


                    #推送钉钉群
                    time_str = str(time.time())[0:10]
                    key = 'smart_address_' + time_str + '.png'
                    img_url = gmt_img_url(key=key, local_file=fig_name)
                    now_time = str(datetime.datetime.now())[0:19]

                    xiaoding = DingtalkChatbot(webhook)
 
                    txt = '【聪明钱地址监控】 @所有人\n' \
                          '> 北京时间%s该地址转出%s个ETH，注意聪明钱正在出货。\n\n' \
                          '> ![数据监控结果](%s)\n'\
                          '> ###### 币coin搜索0xCarson,关注OKX实盘。 \n'%(now_time,str(-change),img_url)

                    xiaoding.send_markdown(title='聪明钱监控', text=txt)
                    #把最新数据写入csv文件中
                    date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df_now = pd.DataFrame({'date':date_now,'address':sub_address,'value':now_value},index=[0]) 
                    df_now = pd.concat([pre_data,df_now])
                    df_now['date'] = pd.to_datetime(df_now['date'])
                    df_now = df_now.sort_values(by='date')
                    df_now = df_now.reset_index(drop=True)
                    df_now = df_now[-10:]
                    #print(df_now)
                    df_now.to_csv('pre_data_1',index=False)
                    logo = 1
                #有btc转入时，余额变多了    
                elif change > 100:
                    # 做图片
                    url ='https://services.tokenview.io/vipapi/address/balancetrend/eth/0x111cff45948819988857bbf1966a0399e0d1141e?apikey=5u0dNQPd55eoEwFPwF2A'
                    session = Session()
                    #url= 'https://services.tokenview.io/vipapi/pending/btc/2e0ec4a6caec1cf2f9cd9e58a5a3954c531d20c08bb88ae8d1a91dc0547f3561?apikey=5u0dNQPd55eoEwFPwF2A'
                    #session.headers.update(headers)
                    try:
                        response = session.get(url)
                        data = json.loads(response.text)
                    except(ConnectionError,Timeout,TooManyRedirects) as e:
                        print(e)
                    date = []
                    ba = []
                    for i in range(len(data['data'])):
                        ins = data['data'][i]
                        date.append(next(iter(ins.keys())))
                        ba.append(float(next(iter(ins.values()))))
                    sub_df = pd.DataFrame({'date':date,'value':ba})
                    url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
                    url_name = ['k_fold']
                    # insert your API key here
                    API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
                    data_list = []
                    for num in range(len(url_name)):
                        print(num)
                        addr = url_address[num]
                        name = url_name[num]
                        # make API request
                        res_addr = requests.get(addr,params={'a': 'ETH', 'api_key': API_KEY})
                        # convert to pandas dataframe
                        ins = pd.read_json(res_addr.text, convert_dates=['t'])
                        #ins.to_csv('test.csv')
                        #print(ins['o'])
                        ins['date'] =  ins['t']
                        ins['value'] =  ins['o']
                        ins = ins[['date','value']]
                        data_list.append(ins)
                    result_data = data_list[0][['date']]
                    for i in range(len(data_list)):
                        df = data_list[i]
                        result_data = result_data.merge(df,how='left',on='date')
                    #last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
                    last_data = result_data[(result_data.date>='2013-01-01')]
                    last_data = last_data.sort_values(by=['date'])
                    last_data = last_data.reset_index(drop=True)
                    print(type(last_data))
                    date = []
                    open_p = []
                    close_p = []
                    high_p = []
                    low_p = []
                    for i in range(len(last_data)):
                        date.append(last_data['date'][i])
                        open_p.append(last_data['value'][i]['o'])
                        close_p.append(last_data['value'][i]['c'])
                        high_p.append(last_data['value'][i]['h'])
                        low_p.append(last_data['value'][i]['l'])
                    #res_data = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})
                    res_data = pd.DataFrame({'date':date,'close':close_p})
                    res_data = res_data.sort_values(by=['date'])
                    res_data = res_data.reset_index(drop=True)
                    sub_df['date'] = pd.to_datetime(sub_df['date'])
                    res_df = sub_df.merge(res_data[['date','close']],how='left',on=['date'])
                    res_df = res_df.sort_values(by='date')
                    import matplotlib.pyplot as plt
                    import seaborn as sns
                    # 绘画折线图
                    f, axes = plt.subplots(figsize=(20, 10))
                    axes_fu = axes.twinx()
                    sns.lineplot(x="date", y="value",color='red', linewidth=0.5,data=res_df, ax=axes)
                    sns.lineplot(x="date", y="close", data=res_df, ax=axes_fu)
                    plt.title('SMART ADDRESS', fontsize=20)

                    title_asset_pool = str(datetime.datetime.now().strftime('%Y-%m-%d'))
                    fig_name = str(title_asset_pool) + '_smart_address' + '.png'
                    plt.savefig(fig_name,bbox_inches='tight')
                    plt.close()


                    #推送钉钉群
                    time_str = str(time.time())[0:10]
                    key = 'smart_address_' + time_str + '.png'
                    img_url = gmt_img_url(key=key, local_file=fig_name)
                    now_time = str(datetime.datetime.now())[0:19]

                    xiaoding = DingtalkChatbot(webhook)
 
                    txt = '【聪明钱地址监控】 @所有人\n' \
                          '> 北京时间%s该地址转入%s个ETH，注意聪明钱正在低位吸货。\n\n' \
                          '> ![数据监控结果](%s)\n'\
                          '> ###### 币coin搜索0xCarson,关注OKX实盘。 \n'%(now_time,str(change),img_url)

                    xiaoding.send_markdown(title='聪明钱监控', text=txt)

                    #把最新数据写入csv文件中
                    date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df_now = pd.DataFrame({'date':date_now,'address':sub_address,'value':now_value},index=[0]) 
                    df_now = pd.concat([pre_data,df_now])
                    df_now['date'] = pd.to_datetime(df_now['date'])
                    df_now = df_now.sort_values(by='date')
                    df_now = df_now.reset_index(drop=True)
                    df_now = df_now[-10:]
                    #print(df_now)
                    df_now.to_csv('pre_data_1',index=False)
                    logo = 1
                #防止粉尘攻击
                else:
                    logo = 1



            else:
                logo = 1

        except(ConnectionError,Timeout,TooManyRedirects) as e:
            logo = 0