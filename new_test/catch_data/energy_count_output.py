#!/usr/bin/env python3.6.0
# -*- coding:utf-8 -*-
# @Author : TIXhjq
# @Time   : 2019/3/10 19:31
import pandas as pd
import datetime
import numpy as np
from pandas import DataFrame

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)


class energy_adhoc(object):
    '''
    important:输入格式:np.array(必要)
        请使用tool.data_to_array进行转换

    energy的相关计算和输出部分.

    '''
    def __init__(self):
        print('Using energy_adhoc backend.')

    # 数据默认list结构

    def consumed_energy_each_node(self, final_energy):
        '''
        :param final_energy:所有能源的数据
        :param init_energy:初始能源数目
        :return: 最大的能源
        '''
        # 花费的能源数据形式
        consum_energy = self.initialenergy - final_energy
        # 花费掉的能源总计
        total_energy = consum_energy.sum()
        # 最大值的数值
        max_energy = np.max(consum_energy)
        # 最大值的位置
        max_energy_rank = np.argmax(consum_energy)
        return consum_energy, total_energy, max_energy, max_energy_rank

    def avg_consumed_energy(self,total_energy):
        '''
        :param total_energy:
        :param n:
        :return: 平均花费的能源
        '''
        return total_energy / self.n

    def std_consumed_energy(self,consumed_energy,avgeragenergy):
        '''
        :param consumed_energy:
        :return: 花费能源标准差数据
        '''
        result=np.sqrt(np.sum((consumed_energy-avgeragenergy)**2)/self.n)
        return result

    def show(self,averagenergy,standard_deviation):
        print("Average Energy Consumption: {}\n".format( averagenergy))
        print("Energy Consumption Standard Deviation: {}\n" .format(standard_deviation))

    def fit(self,final_energy,n,initalenergy,verbosity=False):

        self.initialenergy = initalenergy
        self.n = n

        consumed_energy, total_energy, max_energy, max_energy_rank=self.consumed_energy_each_node(final_energy)
        averagenergy=self.avg_consumed_energy(total_energy)
        standard_deviation=self.std_consumed_energy(consumed_energy,averagenergy)

        if(verbosity):
            self.show(averagenergy, standard_deviation)

        return averagenergy,standard_deviation

if __name__=='__main__':
    pass