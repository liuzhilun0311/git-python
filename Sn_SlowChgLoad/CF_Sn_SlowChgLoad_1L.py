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
task_start_day = 13
task_end_day = 31
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

class caculation_1L:

	# 获取某个月的日期： 输入年月，获得当月日历
	def getCalendarList(year, month):
		list_date = []
		c = Calendar(firstweekday=6)
		for day in c.itermonthdays(year, month):
			if day != 0:
				list_date.append(str(date(year, month, day)).replace("-", ""))
		return list_date

	# 根据日期，获取数据加载地址：/parquet_data/a26_enterprise/vintype=A26/dt=?/*.parquet
	def getDirFileLoadPath(list_date):
		dir_file_load_path = {}
		for date in list_date:
			load_path = "hdfs:/parquet_data/a26_enterprise/vintype=A26/" + "dt=%s" % (str(date))
			file_name = "%s%s" % ("*", ".parquet")
			file_load_path = os.path.join(load_path, file_name)
			dir_file_load_path.setdefault(str(date), str(file_load_path))

		return dir_file_load_path

	# 根据事项获取数据保存地址；数据保存：/user/xs6_query/vintype=A26/caculation_item/...
	def getDirFileSavePath(list_date, caculation_item, caculation_item_1L):
		dir_file_save_path = {}
		dir_file_save_path.setdefault(str(caculation_item), {})
		dir_file_save_path[str(caculation_item)].setdefault(str(caculation_item_1L), {})

		for date in list_date:
			save_path = r"/user/xs6_query/results/%s%s%s%s%s" % (
				str(caculation_item), "/", str(caculation_item_1L), "/dt=", str(date))
			file_save_path = os.path.join(save_path, "")
			dir_file_save_path[str(caculation_item)][str(caculation_item_1L)].setdefault(str(date), str(file_save_path))
		return dir_file_save_path

	logging.info("一级任务的名称:%s" % (str(caculation_item_1L) + "\n"))

	# 定义任务起始日期
	range_task_date = getCalendarList(task_date["task_year"], task_date["task_month"])[
					  (task_date["task_start_day"] - 1):task_date["task_end_day"]]

	logging.info("任务的时间范围:%s" % (
			str(range_task_date[0]) + " 至 " + str(range_task_date[-1]) + "\n"))

	# 定义数据加载和保存路径
	dir_load_path = getDirFileLoadPath(getCalendarList(task_date["task_year"], task_date["task_month"]))
	dir_save_path = getDirFileSavePath(getCalendarList(task_date["task_year"], task_date["task_month"]),
									   caculation_item, caculation_item_1L)

	logging.info("一级任务开始计算...")

	# 定义Spark配置函数
	conf = SparkConf().setAppName(str(caculation_item_1L))

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

		# 建立窗口函数
		window_first = Window.partitionBy(["vin", ]).orderBy(["sampleTime", ]).rowsBetween(0, sys.maxsize)

		window_last = Window.partitionBy(["vin", ]).orderBy(["sampleTime", ]).rowsBetween(-sys.maxsize, 0)

		window_lag_lead = Window.partitionBy(["vin", ]).orderBy(["sampleTime", ])

		window_group = Window.partitionBy(["vin", "group"]).orderBy(["sampleTime"])\
			.rowsBetween(-sys.maxsize, sys.maxsize)

		try:
			# 选取相关的数据
			df_data = spark.read.format('parquet').load(loadPath).select([

				# 数据标识：vin,sampleTime
				col('vin'),  # 车辆vin码,T=1s
				col('sampleTime'),  # 采样时间,T=1s

				# 判断车辆状态数据：READY状态，车速，充电状态，充电枪连接状态
				col("VCU_VehRdySt"),  # 车辆Ready状态，dT = 1s
				col("BCS_VehSpd"),  # 车速，T=1s
				col('VCU_VehChgDischgSt'),  # 车辆充电状态,T=5s
				col('BMS_BattDcChgCcSt'),  # 快充枪连接信号，T=5s
				col('IPS_OBCCC_St'),  # 慢充枪连接信号，T=5s

				#需求相关信号
				col("IPS_OBCCC_Cap"),  # 慢充负载信号，T=30s

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			]).dropDuplicates(["vin", "sampleTime"])

			# 场景相关数据填充为1s数据，进行统一
			df_fill = df_data.select([

				col("vin"),
				col("sampleTime"),

				col("VCU_VehRdySt"),
				col("BCS_VehSpd"),  # 车速，T=1s
				F.last(col('VCU_VehChgDischgSt'), ignorenulls=True).over(window_last).name("VCU_VehChgDischgSt"),
				F.last(col('BMS_BattDcChgCcSt'), ignorenulls=True).over(window_last).name("BMS_BattDcChgCcSt"),
				F.last(col('IPS_OBCCC_St'), ignorenulls=True).over(window_last).name("IPS_OBCCC_St"),

				col('IPS_OBCCC_Cap'),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			]).dropna(subset = ["VCU_VehChgDischgSt","BMS_BattDcChgCcSt","IPS_OBCCC_St"])

			# 定义主场景
			df_Sn = df_fill.select([

				col("vin"),
				col("sampleTime"),

				# 定义场景
				F
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") != 2) & (col("VCU_VehChgDischgSt") != 29) &
					 (col("VCU_VehChgDischgSt") != 8)),
					F.lit(1)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") == 2) | (col("VCU_VehChgDischgSt") == 29)),
					F.lit(2)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					(col("VCU_VehChgDischgSt") == 8),
					F.lit(3)
				)
					.when(
					(col("VCU_VehRdySt") == 1) &
					(col("BCS_VehSpd").isNotNull()),
					F.lit(4)
				)
					.otherwise(0)
					.name("Sn"),

				# 定义场景一级标签
				F
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") != 2) & (col("VCU_VehChgDischgSt") != 29) &
					 (col("VCU_VehChgDischgSt") != 8)),
					F.lit(10)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") == 2) | (col("VCU_VehChgDischgSt") == 29)) &
					((col("IPS_OBCCC_St") == 2) & (col("BMS_BattDcChgCcSt") == 0)),
					F.lit(21)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") == 2) | (col("VCU_VehChgDischgSt") == 29)) &
					((col("IPS_OBCCC_St") == 0) & (col("BMS_BattDcChgCcSt") == 1)),
					F.lit(22)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					(col("VCU_VehChgDischgSt") == 8),
					F.lit(30)
				)
					.when(
					(col("VCU_VehRdySt") == 1) &
					(col("BCS_VehSpd").isNotNull()),
					F.lit(40)
				)
					.otherwise(0)
					.name("Sn_Label_1L"),

				# 定义场景二级标签
				F
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") != 2) & (col("VCU_VehChgDischgSt") != 29) &
					 (col("VCU_VehChgDischgSt") != 8)),
					F.lit(100)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") == 2) | (col("VCU_VehChgDischgSt") == 29)) &
					((col("IPS_OBCCC_St") == 2) & (col("BMS_BattDcChgCcSt") == 0)),
					F.lit(210)
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					((col("VCU_VehChgDischgSt") == 2) | (col("VCU_VehChgDischgSt") == 29)) &
					((col("IPS_OBCCC_St") == 0) & (col("BMS_BattDcChgCcSt") == 1)),
					F.lit(220)  # 车速为0、充电状态，慢枪断开，快枪连接
				)
					.when(
					(col("VCU_VehRdySt") == 0) &
					(col("BCS_VehSpd") == 0) &
					(col("VCU_VehChgDischgSt") == 8),
					F.lit(300)  # 车辆非READY状态，车速为0、放电状态
				)
					.when(
					(col("VCU_VehRdySt") == 1) &
					(col("BCS_VehSpd").isNotNull()),
					F.lit(400)  # READY,车速==0,枪非连接
				)
					.otherwise(0)
					.name("Sn_Label_2L"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			])

			# 定义场景有效性
			df_VD = df_Sn.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),

				F.when((col("Sn") == F.lag(col("Sn"), 1).over(window_lag_lead)) |
					   (col("Sn") == F.lead(col("Sn"), 1).over(window_lag_lead)),
					   F.lit(1)).otherwise(0).name("Sn_VD"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			])

			# 过滤掉无效场景
			df_VD_1 = df_VD.filter(col("Sn_VD") == 1)

			# 定义场景变化量，从而定义场景的起始、结束时间
			df_Sn_diff = df_VD_1.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),
				(col("Sn") - F.lag(col("Sn"), 1).over(window_lag_lead)).name("Sn_diff_last"),
				(col("Sn") - F.lead(col("Sn"), 1).over(window_lag_lead)).name("Sn_diff_next"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			])

			# 场景选择：充电
			df_SelectSn = df_Sn_diff.filter(col("Sn") == 2)

			# 定义场景的起始、过程、结束
			df_Sn_start_end = df_SelectSn.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),
				F.when(
					(col("Sn_diff_last") != 0) | (col("Sn_diff_last").isNull()),
					col("sampleTime")).name("Sn_start_sampleTime"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),
			])

			df_group = df_Sn_start_end.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),
				col("Sn_start_sampleTime"),

				F.last(col('Sn_start_sampleTime'), ignorenulls=True).over(window_last).name("group"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			])

			df_Sned = df_group.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),
				F.from_unixtime(col("Sn_start_sampleTime") / 1000, format='yyyy-MM-dd')
					.name("date"),
				F.from_unixtime(col("Sn_start_sampleTime") / 1000, format='HH:mm:ss')
					.name("Sn_start_time"),

				F.from_unixtime(
					F.last(col('sampleTime')).over(window_group) / 1000, format='HH:mm:ss')
					.name("Sn_end_time"),

				# 过程时间
				F.format_number(
					((F.max(col('sampleTime')).over(window_group) - F.min(col('sampleTime')).over(window_group)) / 3600000),
					4).name("Sn_T"),

				F.count(col("sampleTime")).over(window_group).name("Sn_sample_count"),

				F.format_number(
					(F.count(col("sampleTime")).over(window_group)) /
					((F.max(col('sampleTime')).over(window_group) - F.min(col('sampleTime')).over(window_group)) / 1000 + 1)
					, 5)
					.name("Data_Validity"),

				col("group"),

				col("IPS_OBCCC_Cap"),

				col("TEL_LatitudeDeg"),
				col("TEL_LatitudeMin"),
				col("TEL_LatitudeSec"),

				col("TEL_LongitudeDeg"),
				col("TEL_LongitudeMin"),
				col("TEL_LongitudeSec"),

			])

			# 计算
			df_caculation = df_Sned.select([

				col("vin"),
				col("sampleTime"),
				col("Sn"),
				col("Sn_Label_1L"),
				col("Sn_Label_2L"),
				col("date"),
				col("Sn_start_time"),
				col("Sn_end_time"),
				col("Sn_T"),

				col("Sn_sample_count"),

				col("Data_Validity"),

				# 计算

				F.max(col("IPS_OBCCC_Cap")).over(window_group).name("Sn_SlowChgLoad"),

				F.format_number(
					(col("TEL_LatitudeDeg") + col("TEL_LatitudeMin")/60 + col("TEL_LatitudeSec")/3600) ,
					9).name("TEL_Latitude"),

				F.format_number(
					(col("TEL_LongitudeDeg") + col("TEL_LongitudeMin") / 60 + col("TEL_LongitudeSec") / 3600),
					8).name("TEL_Longitude"),

			])

			# 选取结果数据
			df_start_end = df_caculation.dropna(subset=["Sn_start_time"])

			# 关联信息
			df_overview = spark.read \
				.format("jdbc") \
				.option("url",
						"jdbc:mysql://192.168.44.54:3306/gac_xs6?useUnicode=true&characterEncoding=utf-8&autoReconnect="
						"true&failOverReadOnly=false&useSSL=false") \
				.option("driver", "com.mysql.jdbc.Driver") \
				.option("dbtable", "car_overview") \
				.option("user", "liuzhilun") \
				.option("password", "Gax@q123+") \
				.option("customSchema",
						"vin STRING,vintype STRING,sub_vintype STRING,car_usage INTEGER,province_his STRING,city_his STRING") \
				.load() \
				.select(["vin", "vintype", "sub_vintype", "car_usage", "province_his", "city_his"])

			df_join = df_start_end.join(df_overview,["vin",],how="left")

			df_result_1L = df_join.withColumn("Sn_Count",F.lit(1))

			savePath_1L = dir_save_path[str(caculation_item)][str(caculation_item_1L)][str(date)]

			df_result_1L.write.format("parquet").mode("overwrite").save(savePath_1L)

			logging.info(str(date) + "子任务结果的保存地址:%s" % (str(savePath_1L) + "\n"))

			end_time = datetime.datetime.now()

			logging.info(str(date) + "子任务结束计算时间%s" % (str(end_time) + "\n"))

			df_data.unpersist()

			df_fill.unpersist()

			df_Sn.unpersist()

			df_VD.unpersist()

			df_VD_1.unpersist()

			df_Sn_diff.unpersist()

			df_group.unpersist()

			df_caculation.unpersist()

			df_start_end.unpersist()

			df_overview.unpersist()

			df_join.unpersist()

			df_result_1L.unpersist()

		except:
			continue


	logging.info("一级任务结束计算...")

	spark.stop()

caculation_1L

logging.info("任务完成！！！")

