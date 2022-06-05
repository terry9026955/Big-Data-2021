#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pandas as pd


# In[42]:


df_ratings = spark.read.option("header", "false")     .option("delimiter", "::")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//ratings.dat")

df_ratings.show(5)

df_movies = spark.read.option("header", "false")     .option("delimiter", "::")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//movies.dat")

df_movies.show(5)

#第三個會出現: AnalysisException: Found duplicate column(s) in the data schema: `10`
#把"::"改成" "可輸出結果但不會做分割
df_users = spark.read.option("header", "false")     .option("delimiter", "::")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//users.dat")

df_users.show(5)


# In[43]:


#Q1

#分數遞減
#以下Dataframe中的_c1 is movieID, _c2 is rating

df_ratings_desc = df_ratings.orderBy(['_c2'], ascending = [False]).select('_c1', '_c2')
df_ratings_desc.show()


# In[46]:


#Q2

#以下Dataframe中的第一行(_c1)跟第二行(_c2)是Rating Information的movieID與rating分數(遞減)
#Dataframe第三行(_c1)則是by Users information中相對應的的column

#Users_gender
df_users_gender = df_users.select('_c1')
#Top rated movies grouped by gender
df_ratings_desc.join(df_users_gender).show()

#Users_age
df_users_age = df_users.select('_c2')
#Top rated movies grouped by age
df_ratings_desc.join(df_users_age).show()

#Users_occupation
df_users_occ = df_users.select('_c3')
#Top rated movies grouped by occupation
df_ratings_desc.join(df_users_occ).show()


# In[52]:


#Q3

#左邊的_c2是genres，右邊的_c2是平均分數

#Average Rating Score
df_rating_average = df_ratings_desc.select(mean('_c2'))

#Movie genres information
df_movies_genres = df_movies.select('_c2')
df_movies_genres.join(df_rating_average).show()


# In[ ]:


#Q4


# In[ ]:


#Q5

