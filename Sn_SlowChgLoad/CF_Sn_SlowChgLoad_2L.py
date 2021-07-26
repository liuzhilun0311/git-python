# coding: utf-8
# author: Liu Zhi Lun

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
from calendar import Calendar #导入日历插件
from datetime import date
from operator import add
import logging
import datetime

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql.functions import col

#定义事项的起止时间
global task_year,task_month,task_start_day,task_end_day,task_date
task_year = 2020
task_month = 7
task_start_day = 10
task_end_day = 12
task_date = {"task_year": int(task_year),
			 "task_month": int(task_month),
			 "task_start_day": int(task_start_day),
			 "task_end_day": int(task_end_day),
			 }

# 定义分析事项名称
global caculation_item,caculation_item_1L,caculation_item_2L
caculation_item = "Sn_SlowChgLoad"
caculation_item_1L = str(caculation_item) + "_1L"
caculation_item_2L = str(caculation_item) + "_2L"

# 定义日志 logging.basicConfig(level = logging.DEBUG,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(
	format='%(asctime)s - %(filename)s - %(levelname)s: %(message)s',
	level=logging.INFO,
	filename=str(caculation_item) + '.log',
	filemode='a',
)

class caculation_2L:

	# 获取某个月的日期： 输入年月，获得当月日历
	def getCalendarList(year, month):
		list_date = []
		c = Calendar(firstweekday=6)
		for day in c.itermonthdays(year, month):
			if day != 0:
				list_date.append(str(date(year, month, day)).replace("-", ""))
		return list_date

	# 根据日期，获取数据加载地址：/parquet_data/a26_enterprise/vintype=A26/dt=?/*.parquet
	def getDirFileLoadPath(list_date,caculation_item,caculation_item_1L):
		dir_file_load_path = {}
		for date in list_date:
			load_path = "hdfs:/user/xs6_query/results/" +  str(caculation_item) + "/" + str(caculation_item_1L) + "/" + \
						"dt=%s" % (str(date))
			file_name = "%s%s" % ("*", ".parquet")
			file_load_path = os.path.join(load_path,file_name)
			dir_file_load_path.setdefault(str(date), str(file_load_path))

		return dir_file_load_path

	def saveToMysql(df_mysql, table, mode):

		url = "jdbc:mysql://192.168.44.54:3306/gac_lzl?useUnicode=true&characterEncoding"\
			  "=utf-8&autoReconnect=true&failOverReadOnly=false&useSSL=false"
		df_mysql.write.jdbc(
			url=url,
			table=table,
			mode=mode,
			properties={"driver": 'com.mysql.jdbc.Driver', "user": "liuzhilun", "password": "Gax@q123+"},
		)

	logging.info("二级任务的名称:%s" % (str(caculation_item_2L) + "\n"))

	# 定义任务起始日期
	range_task_date = getCalendarList(task_date["task_year"], task_date["task_month"])[
					  (task_date["task_start_day"] - 1):task_date["task_end_day"]]

	logging.info("任务的时间范围:%s" % (
			str(range_task_date[0]) + " 至 " + str(range_task_date[-1]) + "\n"))

	# 定义数据加载和保存路径
	dir_load_path = getDirFileLoadPath(getCalendarList(task_date["task_year"], task_date["task_month"])
									   ,caculation_item,caculation_item_1L)

	logging.info("二级任务开始计算...")

	# 定义Spark配置函数
	conf = SparkConf().setAppName(str(caculation_item_2L))

	conf.set("spark.executor.cores", "4")  # 每个executor的核心数
	conf.set("spark.executor.instances", "20")  # 集群中启动的executor总数
	conf.set("spark.executor.memory", "20g")  # 每个executor分配的内存数
	conf.set("spark.driver.cores", "4")  # 设置driver的CPU核数
	conf.set("spark.driver.memory", "16g")  # driver的内存大小
	conf.set("spark.driver.maxResultSize", "8g")  # 设置driver端结果存放的最大容量超过不运行
	conf.set("spark.executor.memoryOverhead", "5120")
	conf.set('spark.dynamicAllocation.enabled', "false")
	conf.set('spark.sql.repl.eagerEval.enabled', "true")  # 改进PySpark数据帧显示输出符合Jupyter notebook
	conf.set('spark.sql.execution.arrow.pyspark.enabled', "true")  # 在Spark中用于在JVM和Python进程之间高效地传输数据。
	conf.set('spark.sql.execution.arrow.pyspark.fallback.enabled', "true")
	conf.set('spark.sql.parquet.mergeSchema', "false")
	conf.set('spark.hadoop.parquet.enable.summary-metadata', "false")
	conf.set("spark.debug.maxToStringFields", "1024")
	conf.set("spark.yarn.submit.waitAppCompletion", "false")
	conf.set("spark.default.parallelism", 80)

	# 建立spark
	spark = SparkSession.builder.config(conf=conf).getOrCreate()

	for date in range_task_date:

		start_time = datetime.datetime.now()
		logging.info(str(date) + "子任务开始计算时间%s" % (str(start_time) + "\n"))

		loadPath = dir_load_path[str(date)]

		logging.info(str(date) + "子任务的数据加载地址:%s" % (str(loadPath) + "\n"))

		# 选取相关的数据
		df_data = spark.read.format('parquet').load(loadPath)

		df_result_2L = df_data

		df_mysql = df_result_2L
		table = caculation_item
		mode = "append"
		saveToMysql(df_mysql, table, mode)

		logging.info(str(date) + "子任务结果的保存数据库为:%s" % (str(caculation_item_2L) + "\n"))

		end_time = datetime.datetime.now()

		logging.info(str(date) + "子任务结束计算时间%s" % (str(end_time) + "\n"))

		df_data.unpersist()

		df_result_2L.unpersist()

		df_mysql.unpersist()

	logging.info("一级任务结束计算...")

	spark.stop()

caculation_2L

logging.info("任务完成！！！")

