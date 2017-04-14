import subprocess
import os
import shutil
import glob
from pyspark.sql.functions import *
from datetime import datetime
from pyspark import SparkConf , SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("bigdataproject")
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

subprocess.call(["hdfs","dfs","-mkdir","bxt160230"])
subprocess.call(["hdfs", "dfs", "-put" , "Police_Incidents.csv", "bxt160230"])
subprocess.call(["hdfs", "dfs", "-put" , "Population.csv", "bxt160230"])

#reading csv file put into hadoop
df = sqlContext.read.load('bxt160230/Police_Incidents.csv', format='com.databricks.spark.csv', header='true', inferSchema='true' , parserLib = 'univocity')
#using subprocess to call the hdfs commands
#creating a list of not required columns
notreqcols = ['Call Date Time', 'Offense Entered Time','Offense Entered  Date/Time','Offense Entered Day of the Week','Offense Entered Month','Date of Report','Map Date','Day2 of the Year'
'Time2 of Occurrence','Day2 of the Week','Month2 of Occurence','Year2 of Occurrence','Day1 of the Year','Time1 of Occurrence','Day1 of the Week','Month1 of Occurence'
'Year1 of Occurrence','Location1','Victim Package','Family Offense','CJIS Code','Penal Code','RMS Code','Final UCR','UCR 2 (Pre-RMS)','UCR 1','UCR Disposition'
'Special Report (Pre-RMS)','Element Number Assigned','Reviewing Officer Badge No','Assisting Officer Badge No','Reporting Officer Badge No','Responding Officer #2 Name'
,'Responding Officer #2 Badge No','Responding Officer #1  Name','Responding Officer #1  Badge No','Investigating Unit 1','Investigating Unit 2','Weather','276285-2016',
'Year of Incident', 'Incident Number wo/ Year','Offense Service Number','Watch','Call (911) Problem','Type of Incident','Penalty Class','Street Block','Street Direction',
'Street Name','Apartment Number','Beat', 'Sector', 'Council District', 'Target Area Action Grids', 'Community','Year1 of Occurrence', 'Month1 of Occurence','Date2 of Occurrence'
,'Time2 of Occurrence', 'Day2 of the Year','Offense Entered Year','DPDSworn/Marshalls involved','Complainant Name','Complainant Apartment','Complainant Business Name','Complainant Business Address'
,'Year Assignment','UCR Disposition','Offense Code CC']

# selecting only required columns
df1 = df.select([c for c in df.columns if c not in notreqcols])

# Converting string date and timestamp columns to its respective type
df2 = df1.withColumn("UpdateDate_timestamp", unix_timestamp("Update Date", "MM/dd/yyyy HH:mm").cast("timestamp"))
df2 = df2.withColumn("Date1 of Occurrence", unix_timestamp("Update Date", "MM/dd/yyyy HH:mm").cast("date"))
df2 = df2.withColumn("Starting_timestamp"    ,unix_timestamp("Starting  Date/Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
df2 = df2.withColumn("Ending Date/Time"        ,unix_timestamp("Ending Date/Time" , "MM/dd/yyyy HH:mm").cast("timestamp"))
df2 = df2.withColumn("Date incident created"   ,unix_timestamp("Date incident created", "MM/dd/yyyy HH:mm").cast("date"))
df2 = df2.withColumn("Call_Received_timestamp" ,unix_timestamp("Call Received Date Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
df2 = df2.withColumn("Call Cleared Date Time"  ,unix_timestamp("Call Cleared Date Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
df2 = df2.withColumn("Call Dispatch Date Time" ,unix_timestamp("Call Dispatch Date Time","MM/dd/yyyy HH:mm").cast("timestamp"))
# replacing spaces in column names with underscore
exprs = [col(column).alias(column.replace('/', '')) for column in df2.columns]
df2 = df2.select(*exprs)
exprs = [col(column).alias(column.replace(' ', '_')) for column in df2.columns]
df2 = df2.select(*exprs)
df2.registerTempTable("incidents1")

# Question 2 solution and need to plot
# UDFs for creating the differences in 
def total_seconds(td):
	if(td != None):
		return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
	else:
		return 0

sqlContext.registerFunction("total_seconds" , lambda td: ((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6 ) if (td != None) else 0 )	
sqlContext.registerFunction("time_delta", lambda y,x:total_seconds((datetime.strptime(y, '%m/%d/%Y %I:%M:%S %p') -datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p'))) if x!= None and y!= None and x!= '' and y!= '' and x!= 'UNK' and y!= 'UNK' else None)
df3 = sqlContext.sql("select *, cast(time_delta(Call_Received_Date_Time,Starting__DateTime)as int) as response_time , cast(time_delta(Call_Received_Date_Time ,Update_Date) as int) as resolution_time from incidents1")
df3.registerTempTable("alldata")
sqlContext.sql("drop database if exists bxt160230 cascade")
sqlContext.sql("create database bxt160230")
sqlContext.sql("drop table if exists bxt160230.data_withresponserate")
sqlContext.sql("create table bxt160230.data_withresponserate as select * from alldata")
df4= df3.filter(df3['Zip_Code']>0).groupBy('Zip_Code').agg({'response_time': 'mean'})
df4.write.format('com.databricks.spark.csv').save('/bxt160230/excel2.csv')
avgresponsetime = df3.agg({'response_time': 'mean'}).collect()
df5 = df4.filter(df4['avg(response_time)'] > 270895.18773028551)

##Qustion 3 : 
cols = ['response_time','resolution_time']    
stats = (df3.groupBy().agg(*([stddev_pop(x).alias(x + '_stddev') for x in cols] + [avg(x).alias(x + '_avg') for x in cols])))
df3 = df3.join(broadcast(stats))
exprs = [(df3[x] - df3[x + '_avg']) / df3[x + '_stddev'] for x in cols]
df4 = df3.select(exprs)
df5 = df4.toDF('normalized_responsetime','normalized_resolutiontime')
df5.registerTempTable("incidents2")
sqlContext.sql("drop table if exists bxt160230.incidents")
sqlContext.sql("create table bxt160230.normalized_time as select * from incidents2")
correlation_value = df5.stat.corr('normalized_responsetime','normalized_resolutiontime')
print("the correlation between response time vs resolution time is",correlation_value)
## Value obtained for correlation is : 0.0085953519811180074
## Found no significant correlation between responsetime and resolution time.

## Question 4:
df5 = sqlContext.sql("select Type_of_Location ,Drug_Related_Incident from bxt160230.data_withresponserate where Drug_Related_Incident !='UNK' ")
#sqlContext.sql("select Type_of_Location, count(*) as crimerate from bxt160230.incidents where Drug_Related_Incident ='Yes' and Type_of_Location != '' group by Type_of_Location order by crimerate desc").show(100, False)
df6 = df5.stat.crosstab("Drug_Related_Incident", "Type_of_Location")
df6.show()

##Question 5: 
args = ["hdfs","dfs","-rm","-r","bxt160230/pig_pop"]
subprocess.call(args)
args = ["hive", "-e", "INSERT OVERWRITE LOCAL DIRECTORY '/root/bxt160230' row format delimited fields terminated by '+' select * from bxt160230.data_withresponserate;"]
p = subprocess.call(args)
if(p!=0):
	print("hive script did not run properly")
pat = '/root/bxt160230'
files = [file for file in glob.glob(pat + '/*')]
outfilename = '/root/bxt160230/hive_table.csv'
with open(outfilename, 'wb') as outfile:
	for filename in files:
		if filename == outfilename:
		# don't want to copy the output into the output
			continue
		with open(filename, 'rb') as readfile:
			shutil.copyfileobj(readfile, outfile)
		readfile.close()
outfile.close()
args = ["hdfs", "dfs" ,"-put","/root/bxt160230/hive_table.csv","bxt160230"]
subprocess.call(args)
## Calling the pig code for the next steps.
args = ["pig","corr.pig"]
p= subprocess.call(args)
if(p!= 0):
	print("pig script didnot execute properly")
sc.close()
