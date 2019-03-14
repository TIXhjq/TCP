#!/usr/bin/env python3.6.0
# -*- coding:utf-8 -*-
# @Author : TIXhjq
# @Time   : 2019/3/10 22:15
import pandas as pd
import datetime
import numpy as np
from pandas import DataFrame
from tools import tools

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)

class tcp_cbr(object):
    '''
    important:输入格式:np.array(必要)
        请使用tool.data_to_array进行转换

    tcp和cbr计算和输出部分.
    '''
    def __init__(self):
        print('Using Tcp_Cbr backend.')
        self.tool=tools(False)

    def get_delay(self, start_time, end_time):
        '''
        :param start_time: 开始时间数据
        :param end_time: 截至时间数据
        :return: 时延差数据,时延差数据
        '''
        packet_duation = end_time - start_time
        end_to_end_delay = []
        end_to_end_delay.append(packet_duation[packet_duation > 0])
        end_to_end_delay_val = np.sum(end_to_end_delay)

        return end_to_end_delay, end_to_end_delay_val

    def avg_delay(self, end_to_end_delay_val, receives):
        '''
        :param end_to_end_delay:时延
        :param receives:接受数目
        :return:时延均值
        '''
        return end_to_end_delay_val / receives


    def total_packets(self, routing_packets,receives):
        '''
        :param routing_packets: 路由数据包数目
        :return:总共的包数
        '''
        return routing_packets+receives

    def show(self,receives,routing_packets, Average_Packet_Delivery,Normalised_Routing_Overhead,avg_end_to_end_delay,first_arrival_time,sends):
        print("Totoal packet sends: {}\n".format( sends))
        print("Totoal packet received: {}\n".format( receives))
        print("Average Packet Delivery Ratio: {}\n".format ( Average_Packet_Delivery))
        print("Total routing packets: {}\n".format( routing_packets))
        print("Normalised Routing Overhead: {}\n".format(Normalised_Routing_Overhead))
        print("Average End-to-End Delay: {}\n".format( avg_end_to_end_delay))
        print("First packets arrival time: {}\n".format( first_arrival_time))

    def fit(self,start_time,end_time,receives,routing_packets,first_arrival_time,sends,verbosity=False):
        '''
        :param verbosity: ture输出中间结果 default:False
        '''
        end_to_end_delay, end_to_end_delay_val=self.get_delay(start_time, end_time)
        Avg_end_to_end_delay=self.avg_delay(end_to_end_delay_val, receives)
        Totoal_packets=self.total_packets(routing_packets,receives)

        Average_Packet_Delivery=(receives / sends) * 100
        Normalised_Routing_Overhead=routing_packets / Totoal_packets

        if(verbosity):

            self.show(
                receives=receives,
                routing_packets=routing_packets,
                Average_Packet_Delivery=Average_Packet_Delivery,
                Normalised_Routing_Overhead=Normalised_Routing_Overhead,
                avg_end_to_end_delay=Avg_end_to_end_delay,
                first_arrival_time=first_arrival_time,
                sends=sends,
            )

        return sends,receives,Average_Packet_Delivery,Totoal_packets,Normalised_Routing_Overhead,Average_Packet_Delivery,first_arrival_time

if __name__=='__main__':
    pass
