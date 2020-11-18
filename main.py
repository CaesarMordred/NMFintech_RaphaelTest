import fitting_error as FE
import predicting_error as PE
import Var_Vol_test as varTest
import data_acquire as da
import datetime
import os

#进入路径
os.chdir(r'D:\Work\NMFintech\Test\Test')


#以下为每次测试需要更改的变量

#测试编号
data_id = '20201112_gf_daily_3.TB'
#测试的债券种类，有'TB','CDB','PFB'
test_bond_type = 'TB'
#此处为了部分情况设计，theo_path 为 all bonds comparison file 的路径，有时候数据量过大超过了excel容纳范围会被拆分
#成两个文件，那就填入两个文件，函数内部会自动concat。如果只有单个文件只需填在theo_path1上并把theo_path2设置为None即可
theo_path1 = 'data/'+data_id+'/'+'real-time-data.20200102-20201112.TB.curves.20201112_gf_daily_3.all-bonds-yield-comparison.csv'
theo_path2 = None
#theo_path2 = 'data/'+data_id+'/'+'real-time-data.20201102-20201109.TB.curves.20201109_gf_202011_10min_2.all-bonds-comparison.csv'
#real_path输入真实交易数据，后续可自行用链接mysql的包获取数据或和Josie获取
real_path = 'data/Jan_Nov_5502.xlsx'
#comparison文件
comparison_path = 'data/'+data_id+'/'+'real-time-data.20200102-20201112.TB.comparison.20201112_gf_daily_3 - Copy.csv'
#exact fitting list文件
liquid_file_path = 'data/liquid/tb_exact_fitting_list.csv'
#测试的开始时间，通常为2020-01-02，注意均为大于或小于不包括等于所以是开区间的日期
startTime = datetime.datetime(year=2020, month=1, day=1)
#测试的结束时间
endTime = datetime.datetime(year=2020, month=11, day=10)

#此处调用了函数，返回值均为表格，但以及在各自的函数内tocsv了。set_bond_each_day是获取每天每只债券所属的bucket
da.set_bond_each_day(liquid_path = liquid_file_path, comparison_path = comparison_path,data_id=data_id,test_bond_type=test_bond_type,
                     startTime=startTime, endTime= endTime)
theo_data = da.set_theo_data(theo_path1,theo_path2,data_id,test_bond_type,startTime,endTime)
real_data = da.set_trade_data(real_path,data_id,test_bond_type,startTime,endTime)
fe = FE.cal_FE(theo_data,real_data,data_id,test_bond_type)
pe = PE.cal_PE(theo_data,real_data,data_id,test_bond_type)
var = varTest.cal_var_vol(theo_data,real_data,data_id,test_bond_type)






