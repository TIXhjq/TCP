#!/usr/bin/env python3.6.0
# -*- coding:utf-8 -*-
# @Author : TIXhjq
# @Time   : 2019/3/11 12:09
import pandas as pd
import datetime
import numpy as np
from pandas import DataFrame

import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)
from tools import tools
from energy_count_output import energy_adhoc
from tcp_cbr_count_output import tcp_cbr

class Pipline(object):
    '''
    p.s 原参数意思不变，col :索引同awk $1->1
    流水线：
        将函数组合用于生成最终指标.
    '''

    def __init__(self):
        print('---------------------')
        self.energy_output=energy_adhoc()
        self.tcp_cbr_output=tcp_cbr()
        self.tool=tools()
        self.result=[]
        self.result_cols=[]
        self.result_DataFrame=DataFrame()
        print('---------------------')
        print('Welcome Pipline')
        print('---------------------')

    def tcp_cbr_pipe(self,data,action, trace, type_,action_aim1, action_aim2,trace_aim, type__aim,time_,energy,save_path,packet_id):
        '''
        :param data:数据
        '''
        #init param
        index_list = [action, trace, type_]

        data = data.reset_index()
        self.tool.set_index(data, index_list)

        # s_r
        data1 = self.tool.get_aim_data(data, action_aim1, trace_aim, type__aim)
        data2 = self.tool.get_aim_data(data, action_aim2, trace_aim, type__aim)

        # 找出每个包的实际发送值
        data1 = self.tool.get_time(data1, packet_id)
        data2 = self.tool.get_time(data2, packet_id)
        sends = self.tool.get_len(data1)

        #找出未丢包的数据
        remove_packetID_list,data2 = self.tool.find_drop_id(data1, data2, packet_id)
        drop_data=self.tool.remove_delay_id(data1, remove_packetID_list, packet_id)


        # routing_packets
        self.tool.reset_index(data,True)
        data3 = self.tool.choose_data(data, action, ['s', 'f'])
        data4 = self.tool.choose_data(data3, trace, ['RTR'])
        data5 = self.tool.choose_data(data4, type_, ['AODV','AntHocNet','sara'])


        #create energy data
        new_data,routing_packets_data,fitter_path,routing_path=self.tool.make_energy_data(data1,data2,drop_data,energy,time_,save_path,index_list,data5,packet_id,node_id)
        routing_packets=self.tool.get_len(routing_packets_data)

        return data1,data2,new_data,sends,routing_packets_data,routing_packets,fitter_path,routing_path

    def statistics_tcp(self,data1,data2,time_):
        #获取时间数据(当条数据)
        start_time__data = self.tool.get_col_data(data1, time_)
        end_time__data = self.tool.get_col_data(data2, time_)

        #转换成适合计算模块计算的格式
        start_time__data = self.tool.data_to_array(start_time__data)
        end_time__data = self.tool.data_to_array(end_time__data)

        #获取开始时间和第一次到达的时间
        start_time_ = self.tool.get_col_data(data1, time_)[0]
        first_arrival_time_ = self.tool.get_col_data(data2, time_)[0]

        #运算部分
        receives=self.tool.get_len(data2)

        return start_time_, start_time__data, end_time__data, first_arrival_time_, receives

    def energy(self,data, energy):
        '''
        :param data: 数据
        '''

        energy_data = self.tool.get_col_data(data, energy)
        energy_data = self.tool.data_to_array(energy_data)

        return energy_data
    def fit(self,packet_id,save_path,energy,n,initialenergy,action, trace, type_,action_aim1, action_aim2,trace_aim, type__aim,node_id,time_,read_path=None,read_path_fitter=None,read_path_routing=None,fitter_data=None,routing_data=None,is_initial_data=True,use_cols=None,verbosity=False,is_path=True,is_set_cols=None):
        '''
        :param data:数据
        :param action_aim1: 筛选得目标
        :param action_aim2: 筛选得目标
        :param trace_aim: 筛选得目标
        :param type__aim: 筛选得目标
        :param path:文件路径
        :is_init_path:是否是原始文档 default:True
        :data:在false的情况下可以直接传入数据
        :verbosity: 控制是否输出中间过程
        '''

        #-------------------------------------------------------------------------------------------------------------------
        #read data
        if is_initial_data:
            data = self.tool.tranform_data(read_path,use_cols=use_cols,is_set_cols=is_set_cols)
            print('begin data format processing')
            print('loading.....')
            start_time=time.time()

            data1, data2, new_data, sends,routing_packets_data,routing_packets,fitter_path,routing_path = self.tcp_cbr_pipe(data, action, trace, type_, action_aim1, action_aim2,trace_aim, type__aim, time_, energy, save_path,packet_id)

            print("data format processing later:{}".format(time.time()-start_time))

        else:
            data1, data2, new_data,sends,routing_packets = self.tool.un_init_data_to_form(read_path_fitter=read_path_fitter,read_path_routing=read_path_routing, fitter_data=fitter_data,routing_data=routing_data, is_path=is_path)
            remove_packetID_list,data2 = self.tool.find_drop_id(data1, data2, packet_id)
            self.tool.remove_delay_id(data1, remove_packetID_list, packet_id)
        #-------------------------------------------------------------------------------------------------------------------
        #tcp_cbr train

        print('begin tcp_cbr count')
        print('loading')
        start_time= time.time()

        start_time_, start_time__data, end_time__data, first_arrival_time_, receives=self.statistics_tcp(data1,data2,time_)

        sends, receives, Average_Packet_Delivery, Totoal_packets, Normalised_Routing_Overhead, Average_Packet_Delivery, first_arrival_time_=self.tcp_cbr_output.fit(
            start_time=start_time__data,
            end_time=end_time__data,
            receives=receives,
            routing_packets=routing_packets,
            first_arrival_time=first_arrival_time_,
            sends=sends,
            verbosity=verbosity
        )
        print('tcp_cbr count later:{}'.format(time.time()-start_time))
        print('---------------------')

        #-------------------------------------------------------------------------------------------------------------------
        #energy train

        print('begin energy count')
        print('loading')
        start_time=time.time()
        new_data=self.tool.get_time(new_data,node_id)
        final_energy=self.energy(data=new_data,energy=energy)
        averagenergy, standard_deviation=self.energy_output.fit(
            final_energy=final_energy,
            n=n,
            initalenergy=initialenergy,
            verbosity=verbosity
        )

        print('energy count later:{}'.format(time.time()-start_time))
        print('---------------------')

        self.result=[averagenergy,standard_deviation,sends, receives, Average_Packet_Delivery, Totoal_packets, Normalised_Routing_Overhead, Average_Packet_Delivery, first_arrival_time_]
        self.result_cols=['averagenergy','standard_deviation','sends', 'receives', 'Average_Packet_Delivery', 'Totoal_packets', 'Normalised_Routing_Overhead', 'Average_Packet_Delivery', 'first_arrival_time_']
        self.result_DataFrame=DataFrame(columns=self.result_cols,data=np.array(self.result).reshape((1,-1)))

        if(is_initial_data):
            self.result_DataFrame['time_Interval']=['all']
            return fitter_path,routing_path




if __name__=='__main__':
    #e.g
    read_path='.//data//AODV_tcp.tr'
    save_path='.//data//test'

    pipline=Pipline()
    n = 40

    energy = '14'
    initialenergy = 10000

    #param
    action = '1'
    action_aim1 = 's'
    action_aim2 = 'r'

    time_ = '2'
    packet_id = '6'

    trace = '4'
    trace_aim = 'AGT'

    type_ = '7'
    type__aim ='tcp'

    node_id='3'

    use_cols=[action,energy,time_,trace,packet_id,type_,node_id]
    #train
    fitter_path,routing_path=\
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
        save_path=save_path,
        is_initial_data=True,
        verbosity=True,
        packet_id=packet_id,
        node_id=node_id,
        use_cols=use_cols,
        is_set_cols=True,
    )
    print(pipline.result_DataFrame)
    pipline.result_DataFrame.to_csv('.//data//basic//first_all_result.csv',index=None)
    print(fitter_path,routing_path)
