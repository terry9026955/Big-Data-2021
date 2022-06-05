#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions 


# In[13]:


df = spark.read.option("header", "true")     .option("delimiter", "::")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//ratings.dat")

df.show(5)

df2 = spark.read.option("header", "true")     .option("delimiter", "::")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//movies.dat")

df2.show(5)

df3 = spark.read.option("header", "true")     .option("delimiter", " ")     .option("inferSchema", "true")     .csv("C://Users//user//Desktop//BigDataHW4//movies//users.dat")

df3.show(5)


# In[ ]:


#Sorry, but I have no idea about this homework. QAQ


# In[ ]:




