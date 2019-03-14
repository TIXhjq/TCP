# TCP
1.运行Pipline 生成全部结果，返回格式转换后的两个文件，自动生成一个本次的结果保存到csv中
2.设置切分数，格式转换后的两个文件的路径，设置步长，和按照步长索引的列名

tcp_cbr_count_output：tcp计算和输出模块
energy_count_output：能源得计算和输出模块
tools: 一些内部函数
Pipline：用来组合计算和工具得模块实现功能

is_iter:控制是否迭代
chunk_size:控制每次大小 default:5000

important:
      如果单独使用计算模块一定要使用tool中得data_to_array进行转换.
      数据数据输入前请确保tcp之类得指标存在，不存在得话会导致索引错误，这个我之前没注意到，导致现在有点问题，现在还懒得改了emm
             
      
