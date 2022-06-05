#!/usr/bin/env python
# coding: utf-8

# In[2]:


#把基本前置作業該裝的裝好

import pandas as pd
import numpy as np
import math
import csv
import re
import json
import time
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions 
from pyspark.sql import Row
from collections import OrderedDict
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T


# In[3]:


#額外需要的功能

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))


# In[57]:


#Dataframe of Covid-19 dataset

#檔案路徑
#'C://Users//user//Desktop//BigDataFinal//us-counties.csv//us-counties.csv'

#讀取.csv檔案並轉為dataframe
data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', 
inferschema='true').load('C://Users//user//Desktop//BigDataFinal//us-counties.csv//us-counties.csv')

data.show(20)


# In[4]:


#用pandas看資料
pdf = pd.read_csv('C://Users//user//Desktop//BigDataFinal//us-counties.csv//us-counties.csv')
pdf.head(20)


# In[5]:


#data.registerTempTable('covid_data')


# In[5]:


data.printSchema()


# In[6]:


#dataframe的總資料量(row)
data.count()


# In[58]:


#最後一天為4/21
latest = data.agg(F.max('date').alias('max_data'))
latest.show()

type(latest)


# In[59]:


#使用data.filter(data[" "] == " ")來過濾出特定日期

#過濾出最後一天(2020/4/21)並show出來
data_filtered = data.filter(data["date"] == "2020-04-21")
data_filtered.show(5)

#2020/4/21當天的資料量
data_filtered.count()


# In[62]:


#Group & Aggregation

#統計所有dataframe的數據資料

#總共的數據
overall_stats = data.agg(
    F.sum("cases").alias("total_cases"), #總計所有的cases
    F.sum("deaths").alias("total_deaths"), #總計所有的deaths
    F.count("*").alias("number_of_records"), # 總計dataset中所有的records數量
    F.countDistinct("county").alias("number_of_counties"), #總計counties的數量
    F.countDistinct("state").alias("number_of_states") #總計states的數量
)
overall_stats.show()

#單日(4/21)的數據
overall_stats_0421 = data_filtered.agg(
    F.sum("cases").alias("2020/04/21_cases"), 
    F.sum("deaths").alias("2020/04/21_deaths"), 
    F.count("*").alias("2020/04/21_number_of_records"), 
    F.countDistinct("county").alias("2020/04/21_number_of_counties"), 
    F.countDistinct("state").alias("2020/04/21_number_of_states")
)
overall_stats_0421.show(1, False)


# In[63]:


#以2021/4/21為例子
#由案例的多至少來排序

data_filtered.orderBy(desc("cases")).show(10, False)


# In[77]:


#2020/4/21的資料
#Grouping data at 'county' level,可看到當日的每個郡的統計數據資料

county_summary = data_filtered.groupBy("county").agg(
    F.sum("cases").alias("total_cases"),
    F.sum("deaths").alias("total_deaths"),
    F.count("*").alias("number_of_records"),
    F.countDistinct("state").alias("number_of_states")
)

#以'county'做少至多的排序(alphabetical order)
county_summary.orderBy("county").show(10, False)


# In[ ]:


#Join的部分
#感覺都是練習而已不確定要不要講


# In[68]:


# filter data for county - Adair & Addison and prepare our left_table
left_table = county_summary.where(
    "county in ('Adair', 'Addison')"
)
left_table.show(5, False)


# In[69]:


# filter data for county - Ada & Accomack and prepare our right_table
right_table = county_summary.where(
    "county in ('Ada', 'Accomack')"
)
right_table.show(5, False)


# In[70]:


# applying inner join
# county_summary contains all the counties and their statistics
inner_table = county_summary.join(
    left_table,
    on=["county"],
    how="inner"
)

inner_table.show(100, False)


# In[71]:


# applying left join
left_table_joined = left_table.join(
    right_table,
    on=["county"],
    how="left"
)

left_table_joined.show(100, False)


# In[72]:


# applying right join
right_table_joined = left_table.join(
    right_table,
    on=["county"],
    how="right"
)

right_table_joined.show(100, False)


# In[73]:


# applying outer join or full join
outer_table_joined = left_table.join(
    right_table,
    on=["county"],
    how="outer"
)

outer_table_joined.show(100, False)


# In[74]:


# get percentage # of cases for each state in Adair
# filter data for county - Adair
adir_overall = county_summary.where(
    "county in ('Adair')"
)
adir_overall.show(5, False)


# In[75]:


# join the Adair summary to its states
perc_cases_statewise = data_filtered.join(
    adir_overall,
    on="county",
    how="inner"
)

perc_cases_statewise.show(10, False)


# In[76]:


# calculate percentage of cases and deaths
perc_cases_statewise = perc_cases_statewise.withColumn(
    "perc_cases",
    F.col("cases")/F.col("total_cases")
).withColumn(
    "perc_deaths",
    F.col("deaths")/F.col("total_deaths")
)

perc_cases_statewise.show(10, False)


# In[ ]:


#parition by & window function
#這部分可以講一下結果


# In[78]:


#要藉由 clause 來 apply parition
from pyspark.sql.window import Window


# In[79]:


#County 的 grouping data 的 dataframe 的結構長這樣
county_summary.show(2, False)


# In[80]:


#建立新的 column 以及存取新的 dataframe 作為 'county_summary_ranked'
#使用 withColumn 來建立新的 columns, 其中第一個參數為 'column name'、第二個參數為 'column value'
#'rank' 就是依照 'column value(total_cases)' 做少至多的排序

county_summary_ranked = county_summary.withColumn(
    "rank", # column name
    # rank function assigns a rank starting from 1 and orderBy condition mentions that rank will be based on the column - total_cases
    # by default the sorting is done in ascending order
    F.rank().over(Window.orderBy("total_cases")) 
)

# print out some of the records
county_summary_ranked.show(5, False)


# In[81]:


#'rank_desc' 就是依照 'column value(total_cases)' 做多至少的排序， 跟 'rank'相反

county_summary_ranked = county_summary_ranked.withColumn(
    "rank_desc",
    # if we want high end values to be assigned the lower rank, we sort the data in descending order using F.desc function
    F.rank().over(Window.orderBy(F.desc("total_cases")))
)

# also, let's order the data by total_cases in descending order
county_summary_ranked.orderBy("total_cases", ascending=False).show(5, False)


# In[83]:


#接下來要為每個郡單獨去創建一個州的排名
#使用2020/4/21當天的資料作範例

# to achieve this we will use the state level dataset containing the data for the latest date
data_filtered.show(2, False)

# also, let's check the count
data_filtered.count()


# In[85]:


# 建立新的 dataframe 叫做 ranked_states
ranked_states = data_filtered.withColumn(
    "state_rank", #county 名稱重複會累加
    #我們還要來指定 partition(劃分) clause(條款), 來為每個郡分別的創造 rank
    F.rank().over(Window.partitionBy("county").orderBy(F.desc("cases"))) 
)

#Ordered by county & state_rank
ranked_states.orderBy("county", "state_rank").show(30, False)


# In[86]:


# now, let's filter out only the top state for each of the county
ranked_states_filtered = ranked_states.filter(
    # since, the top state has been assigned rank-1, we can filter states where state_rank is 1
    "state_rank = 1"
)

# let's check the count which should be equal to the number of counties in the dataset
ranked_states_filtered.count()


# In[87]:


# also, let's check the results
ranked_states_filtered.orderBy("county", "state_rank").show(50, False)


# In[88]:


# assigning the parition by clause in a variable - w
partition_clause = Window.partitionBy("county")


# In[89]:


# let's now get the total, average and maximum county cases across each of their respective states
# we can use .withColumn in a chain format, that is one after the other and all are executed in a sequential order
ranked_states = ranked_states.withColumn(
    "country_total_cases",
    # we will use the parition_clause we created above
    F.sum("cases").over(partition_clause)
).withColumn(
    # here we are calculating the % of cases in a state i.e. state_cases/respective_county_cases
    "perc_total_cases",
    F.col("cases")/F.col("country_total_cases")
).withColumn(
    "country_avg_cases",
    # get county average cases
    F.avg("cases").over(partition_clause)
).withColumn(
    # get county max caaes
    "country_max_cases",
    F.max("cases").over(partition_clause)
)

# now, let's print out the results
ranked_states.orderBy("county", "perc_total_cases").show(50, False)


# In[ ]:




