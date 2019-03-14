#!/usr/bin/env python3.6.0
# -*- coding:utf-8 -*-
# @Author : TIXhjq
# @Time   : 2019/3/12 21:38
import pandas as pd
import datetime
import numpy as np
from pandas import DataFrame
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)

from tools import tools
from Piplen import Pipline

read_path = './/data//test.tr'
read_path_fitter = './/data//test_fit.csv'
read_path_routing = './/data//test_pack.csv'
save_path = './/data//test'

action = '1'
energy = '14'
action_aim1 = 's'
action_aim2 = 'r'

time_ = '2'
packet_id = '6'

trace = '4'
trace_aim = 'AGT'

node_id='3'

type_ = '7'
type__aim = 'tcp'
step=50

def train(is_path=None,fitter_data=None,routing_data=None):

    n = 40
    initialenergy = 10000

    pipline = Pipline()
    # train
    pipline.fit(
            action=action,
            trace=trace,
            type_=type_,
            action_aim1=action_aim1,
            action_aim2=action_aim2,
            trace_aim=trace_aim,
            type__aim=type__aim,
            time_=time_,
            energy=energy,
            initialenergy=initialenergy,
            n=n,
            read_path=read_path,
            read_path_fitter=read_path_fitter,
            read_path_routing=read_path_routing,
            save_path=save_path,
            is_initial_data=False,
            verbosity=False,
            is_path=is_path,
            fitter_data=fitter_data,
            routing_data=routing_data,
            packet_id=packet_id,
            node_id=node_id,
        )
    return pipline.result,pipline.result_cols

tool=tools()
data=pd.read_csv('.//data//test_3.csv ')
data2=pd.read_csv('.//data//test_routing_packet.csv')

fit_data=tool.split_data_step(data,step=step,by_col=time_,is_by_col=True)
pack_data=tool.split_data_step(data2,step=step,by_col=time_,is_by_col=True)

all_result=[]
cols_=0

count=0
start_time=time.time()
for fitter_data,routing_data in zip(fit_data,pack_data):
    print('{} of begin'.format(count))

    result,cols=train(is_path=False,fitter_data=fitter_data,routing_data=routing_data)
    all_result.append(result)
    cols_=cols

    print('{} of end later:{}'.format(count,time.time()-start_time))
    count+=1
print('create result time:{}'.format(time.time()-start_time))

print('save all result')
start_time=time.time()

new_data=DataFrame(data=all_result,columns=cols_)
new_data['time_Interval']=range(0,tool.get_len(new_data)*step,step)
new_data.to_csv('.//data//basic//step_50_result.csv',index=0)

print('save result later:{}'.format(time.time()-start_time))