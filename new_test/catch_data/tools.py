#!/usr/bin/env python3.6.0
# -*- coding:utf-8 -*-
# @Author : TIXhjq
# @Time   : 2019/3/11 11:18
import pandas as pd
import datetime
import numpy as np
from pandas import DataFrame
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)

import re
import gc

class tools(object):
    '''
    工具包:
        主要用在Pipline中的一些内部函数，
        后几个声明为专有函数，无泛化能力.
    '''
    def __init__(self,show=True):
        if show:
            print("Using Tools backend.")

    def formal_change(self,data):
        '''
        :param data: 数据
        :return: 适合得数据，减小数据规模
            筛选指标:s,r,f
        '''
        new_data = []
        for line in data:
            if line[0][0] in ['s', 'r', 'f']:
                new_line = re.split(" ", line[0])
                new_data.append(new_line)
        return new_data

    def set_index(self,data, index_list):
        '''
        :param data: 数据
        :param index_list: 需要设置索引得列名 e.g:[1,2]
        '''
        data.set_index(index_list, inplace=True)
        data.sort_index(inplace=True)

    def get_aim_data(self,data, action_aim,trace_aim,type_aim):
        '''
        important: 必须遵循索引得顺序
        :param data: 数据
        :param key_list: 通过索引寻找目标
        :return: 找到得数据
        '''
        new_data = data.loc[action_aim,trace_aim,type_aim]
        self.reset_index(new_data,inplace=True)
        return new_data

    def reset_index(self,data,inplace):
        '''
        :param data:数据
        :return: 取消索引
        '''
        data.reset_index(inplace=inplace)

    def get_len(self,data):
        '''
        :param data: 数据
        :return: 数据得长度
        '''
        len = data.shape[0]
        return len

    def str_to_int(self,data):
        '''
        :param list: 含有字符型的数字
        :return: 转换类型之后数据
        '''
        new_data=[]
        for data_ in data:
            new_data.append(float(data_))
        return new_data

    def get_max_col(self,data, col):
        '''
        :param data: 数据
        :param col: 列名 e.g:$1-1
        :return: 列名对应的数据中的最大值
        '''
        list = data[col].values.tolist()
        list = self.str_to_int(list)
        max = np.max(list)
        return max

    def get_col_data(self,data, col):
        '''
        :param data: 数据
        :param col: 列名
        :return: 对应列名的数据
        '''
        return data[col]

    def data_to_array(self, data, is_str_to_list=True):
        '''
        important:计算输出类的函数默认array类型，使用其类前务必进行转换
        :param data: 将DataFrame转换成Array类型方便运算
        :param is_str_to_list: 是否存在字符数字，需要将其转换.default(True)
        :return: 转换后的数据
        '''
        if (is_str_to_list):
            return np.array(self.str_to_int(data.values.tolist()))
        else:
            return np.array(data.values.tolist())

    def choose_data(self,data, col, if_):
        '''
        :param data: 数据
        :param col: 列名
        :param if_: 用过条件选择数据，只支持==
        :return: 需要的数据
        '''

        right_key=[]
        for i in if_:
            if i in data[col].unique().tolist():
                right_key.append(i)
        if_=right_key

        test=data.groupby([col])
        new_data=DataFrame()
        for i in if_:
            test1=test.get_group(i)
            new_data=pd.concat([new_data,test1])

        return new_data

    def choose_key(self,data, key_word):
        '''
        :param data: 数据
        :param key_word: 关键字，可以利用make_pattern生成，或者直接传入pattern
        :return: 根据关键字搜索到的数据
        '''
        new_data = []
        for i in data:
            if(re.search(string=i[0],pattern=key_word)):
                new_data.append(i.tolist())
        del data
        gc.collect()
        return new_data

    def save_data(self,data, path, file_name,is_index=None,is_header=True):
        '''
        :param data: 数据
        :param path: 希望保存的路径
        :param file_name: 文件名字
        :return: 保存文件的路径
        '''
        data_cols=data.columns.tolist()
        data=data.values
        data=DataFrame(columns=data_cols,data=data)
        path=path + '_' + file_name + '.csv'
        data.to_csv(path, index=None)
        data=pd.read_csv(path)
        return path

    def make_pattern(self,key_word):
        '''
        之前想法，现在暂时无用
        :param key_word: 字符串中顺序的关键字的列表
        :return: 寻找指定字符串的re的pattern
        '''
        new_string=''
        for key in key_word:
            new_string+=key+'.*?'
        return new_string

    def remove_delay_id(self,data,need_remove_,index_col):
        '''
        :param data: 数据
        :param need_remove_:需要删除的行中，所在index_col列中的数据名称
        :param index_col: 用来做索引的列
        :return: 丢失的数据
        '''
        self.set_index(data,index_col)
        drop_data=data.loc[need_remove_]
        data.drop(need_remove_,inplace=True)
        drop_data.reset_index(inplace=True)
        data.reset_index(inplace=True)
        return drop_data

    def split_data_step(self,data,step,by_col=None,is_by_col=False):
        '''
        important 还原索引
        :param data: 数据
        :param step: 选择步长
        :保存的地址并不是完整的 e.g './/data//test' default:.csv
        :param by_col:设置步长列所在的列名
        :is_by_ocl:是否根据列名设置步长 default:False 根据索引设置
        :return: 期望数据,所有的保存路径
        '''
        if not is_by_col:
            max=self.get_len(data)
        if is_by_col:
            max=self.get_max_col(data,by_col)

        if max>int(max):
            max=int(max)
        else:
            max=int(max-1)
        step_list_begin=range(0,max+step,step)
        step_list_end=range(step,max+step,step)

        print('begin split data')
        print('loading.....')
        # start_time = time.time()s

        # count=1
        # all_path=[]
        all_data=[]
        for begin,end in zip(step_list_begin,step_list_end):
            new_data=data[data[by_col]>=begin]
            new_data=new_data[new_data[by_col]<end]
            all_data.append(new_data)

            # path=self.save_data(new_data,path=save_path,file_name=str(end))
            # all_path.append(path)

            # print('save {} data end:{}'.format(count,time.time()-start_time))
            # count+=1

        # print('save end:{}'.format(time.time()-start_time))

        return all_data

    def find_drop_id(self,start_data,end_data,packet_id):
        '''
        特殊函数...
        只能用来寻找丢包的packet_id
        :param start_data: 开始时间
        :param end_data: 结束时间
        :param packet_id: 包名
        :return: 丢包的包名
        '''
        start_id=start_data[packet_id].unique().tolist()
        end_id=end_data[packet_id].unique().tolist()

        delay_id=[]
        for end in end_id:
            if not end in start_id:
                delay_id.append(end)
        self.remove_delay_id(end_data,delay_id,packet_id)
        end_id=end_data[packet_id].unique().tolist()

        delay_id=[]
        for start in start_id:
            if not start in end_id:
                delay_id.append(start)

        if(delay_id==[]):
            end_data.set_index([packet_id],inplace=True)
            end_data=end_data.loc[start_id]
            end_data.reset_index(inplace=True)

        return delay_id,end_data

    def get_time(self,data1,packet_id,rank=-1):
        '''
        特殊函数只是用来寻找time和receives
        其他没什么用处，如无特用，不用改变rank

        :param data1: 数据
        :param packet_id: 包名
        :param rank: default:-1
        :return: 根据报名筛选出的数据，和重复数据长度
        '''
        # test
        cols = data1.columns.tolist()
        test = data1.groupby([packet_id])

        all_start = []
        all_pack_id=data1[packet_id].unique().tolist()
        for i in data1[packet_id].unique().tolist():
            test2 = test.get_group(i)
            all_start+=test2.iloc[rank:, ].values.tolist()

        data1 = DataFrame(data=all_start, columns=cols)

        return data1

    def make_energy_data(self,start_data,end_data,drop_data,key,time_,save_path,index_list,routing_packet,packet_id,file_name_fitter='fitter_data',file_name_routing='routing_packet',index='index',node_id='3'):
        '''
        特殊函数:用于生成适合energy的数据集
        :param start_data:开始数据
        :param end_data: 结束数据
        :param key: 关键字 energy
        :param index:因为将索引还原过故列表始终存在一列，作为标识符，列名'index'
        :return: energy数据
        '''
        start_data['judge_start']=1
        end_data['judge_start']=0
        drop_data['judge_start']=-1

        start=start_data[[key,index,time_,'judge_start']+index_list+[packet_id,node_id]]
        end=end_data[[key,index,time_,'judge_start']+index_list+[packet_id,node_id]]
        drop=drop_data[[key,index,time_,'judge_start']+index_list+[packet_id,node_id]]
        routing_packet_time_data=DataFrame(data=routing_packet[time_].tolist(),columns=[time_])

        new_data=pd.concat([start,end,drop])
        new_data.sort_values(by=index,inplace=True)
        fitter_path=self.save_data(new_data,save_path,file_name_fitter)
        routing_path=self.save_data(routing_packet_time_data,save_path,file_name_routing)

        self.set_index(new_data,index_list)
        return new_data,routing_packet_time_data,fitter_path,routing_path

    def un_init_data_to_form_loc(self,judge_start_num,data):
        data=data.get_group(judge_start_num)
        data=DataFrame(data)
        data.reset_index(inplace=True)
        remove_col=data.columns.tolist()[0]
        data.drop(columns=remove_col,inplace=True)
        return data

    def un_init_data_to_form(self,read_path_fitter=None,read_path_routing=None,fitter_data=None,routing_data=None,is_path=True,is_iter=False,chunk_size=0):
        '''
        :is_iter:是否迭代读入
        :chunk_size:每次迭代的大小
        :param data:数据
        :param read_path: 非原始数据的地址
        :param is_path:是否为地址形式传入 default:True
        :return: 开始时间数据，结束时间数据，完整数据，发送数
        '''
        if is_path:
            if is_iter:
                fitter_data=pd.read_csv(read_path_fitter)
                routing_data=pd.read_csv(read_path_routing)
        if is_path:
            if is_iter:
                routing_data=self.iterm_read_data(read_path_routing,is_iter,chunk_size)
                fitter_data=self.iterm_read_data(read_path_fitter,is_iter,chunk_size)

        type_=fitter_data['judge_start'].unique().tolist()
        test = fitter_data.groupby(['judge_start'])
        start_data = self.un_init_data_to_form_loc(1,test)
        if -1 in type_:
            drop_data = self.un_init_data_to_form_loc(-1,test)
            sends = self.get_len(start_data) + self.get_len(drop_data)
        else:
            sends=self.get_len(start_data)
        end_data = self.un_init_data_to_form_loc(0,test)

        routing_packet=self.get_len(routing_data)

        return start_data,end_data,fitter_data,sends,routing_packet

    def iterm_read_data(self,read_path,is_iterator=True,chunk_size=5000):
        data = pd.read_table(read_path, header=None, iterator=is_iterator)

        loop = True
        chunkSize = chunk_size
        chunks = []
        count = 1
        while loop:
            try:
                print("read data {}".format(count))
                print("loading...")
                chunk = data.get_chunk(chunkSize)
                chunk = chunk.values.tolist()
                chunk = self.formal_change(chunk)
                chunk = DataFrame(data=chunk)
                chunks.append(chunk)
                print("read data {} end".format(count))
                print('---------------------------------')
                count += 1
            except StopIteration:
                loop = False

        print('Iteration is stopped')
        print('concat')
        new_data = pd.concat(chunks, ignore_index=True)
        del chunks, data
        gc.collect()

        print('end_cat')
        data = DataFrame(data=new_data)

        return data

    def tranform_data(self,read_path,use_cols=None,is_set_cols=False,chunk_size=5000,is_iterator=True):
        '''
        :param read_path: 原始数据地址
        :return: 适合的数据
        '''
        print('read data')
        print('loading....')
        start_time=time.time()
        if not is_iterator:
            data = pd.read_table(read_path, header=None, iterator=is_iterator)
            data = data.values.tolist()
            data = self.formal_change(data)
            data = DataFrame(data=data)
        if is_iterator:
            data=self.iterm_read_data(
                read_path=read_path,
                is_iterator=is_iterator,
                chunk_size=chunk_size
            )
        l = len(data.columns.tolist())
        data.drop(columns=[4], inplace=True)
        cols=range(1,l)
        cols = [ str(x) for x in cols ]
        data.columns=cols
        if is_set_cols:
            data=data[use_cols]

        print('read data later:{}'.format(time.time()-start_time))
        print('---------------------')
        return data