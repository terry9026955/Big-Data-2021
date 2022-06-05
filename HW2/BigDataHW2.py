#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *


# In[2]:


#讀檔案

data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', 
inferschema='true').load('C://Users//user//Desktop//BigDataHW2//News_Final.csv')
data.show(5)

#取Title跟Headline的dataframe
data1 = data.select("Title", "Headline")
data1.show()


# In[23]:


#第一題之不正確寫法

#Title
data1.groupBy("Title")     .count()     .orderBy(col("count").desc())     .show()

#Headline
data1.groupBy("Headline")     .count()     .orderBy(col("count").desc())     .show()


# In[17]:


#第一題之較正確的寫法
from pyspark.sql.functions import explode, split, create_map, count, max, col

#先把四個Topic分開
df_oba = data.filter(data["Topic"] == "obama")
df_eco = data.filter(data["Topic"] == "economy")
df_mic = data.filter(data["Topic"] == "microsoft")
df_pal = data.filter(data["Topic"] == "palestine")

#obama topic
df_oba.select(col("Topic"), explode(split(df_oba.Title, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)
df_oba.select(col("Topic"), explode(split(df_oba.Headline, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)


# In[18]:


#economy topic
df_eco.select(col("Topic"), explode(split(df_eco.Title, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)
df_eco.select(col("Topic"), explode(split(df_eco.Headline, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)


# In[22]:


#microsoft topic
df_mic.select(col("Topic"), explode(split(df_mic.Title, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)
df_mic.select(col("Topic"), explode(split(df_mic.Headline, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)


# In[20]:


#palestine topic
df_pal.select(col("Topic"), explode(split(df_pal.Title, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)
df_pal.select(col("Topic"), explode(split(df_pal.Headline, " ")).alias('words')).groupBy("Topic", "words").count().orderBy(desc("Topic"), desc("count")).take(20)


# In[13]:


#第二題 
from functools import reduce

data_dir = "C://Users//user//Desktop//BigDataHW2//"

topics_name = ["Economy", "Microsoft", "Obama", "Palestine"]
platforms_name = ["Facebook", "GooglePlus", "LinkedIn"]
paths = [list(map(lambda x: data_dir + platform + "_" + x + ".csv", topics_name)) for platform in platforms_name]

dfs = [list(map(lambda x: spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(x), path)) for path in paths]

dfs = [reduce(DataFrame.union, platform_df) for platform_df in dfs]

k = 60 // 20
dfs_hours = [df.select(col("IDLink"), *[(reduce(lambda x, y: x + y, [col(df.columns[j]) for j in range(1+k*i, 1+k*i+k)])/k).alias(f"hour{i}") for i in range(0, len(df.columns)//k)]) for df in dfs]

k = 24 * 60 // 20
dfs_days = [df.select(col("IDLink"), *[(reduce(lambda x, y: x + y, [col(df.columns[j]) for j in range(1+k*i, 1+k*i+k)])/k).alias(f"day{i}") for i in range(0, len(df.columns)//k)]) for df in dfs]


# In[26]:


#第三題

data2 = data.select("Topic", "SentimentTitle", "SentimentHeadline")


#obama
sentiment_score_oba = data2.filter(data2["Topic"] == "obama").select("Topic", "SentimentTitle","SentimentHeadline")
sentiment_score_oba.groupby("Topic").agg(sum("SentimentTitle"), mean("SentimentTitle"), sum("SentimentHeadline"), mean("SentimentHeadline")).show()

#economy
sentiment_score_eco = data2.filter(data2["Topic"] == "economy").select("Topic", "SentimentTitle","SentimentHeadline")
sentiment_score_eco.groupby("Topic").agg(sum("SentimentTitle"), mean("SentimentTitle"), sum("SentimentHeadline"), mean("SentimentHeadline")).show()

#microsoft
sentiment_score_mic = data2.filter(data2["Topic"] == "microsoft").select("Topic", "SentimentTitle","SentimentHeadline")
sentiment_score_mic.groupby("Topic").agg(sum("SentimentTitle"), mean("SentimentTitle"), sum("SentimentHeadline"), mean("SentimentHeadline")).show()

#palestine
sentiment_score_pal = data2.filter(data2["Topic"] == "palestine").select("Topic", "SentimentTitle","SentimentHeadline")
sentiment_score_pal.groupby("Topic").agg(sum("SentimentTitle"), mean("SentimentTitle"), sum("SentimentHeadline"), mean("SentimentHeadline")).show()


# In[ ]:


#第四題 

