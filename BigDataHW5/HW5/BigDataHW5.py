#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import math
import csv
import re
import json
import time
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions 
from pyspark.sql import Row
from collections import OrderedDict
from pyspark.sql.functions import *


# In[2]:


# read data

# data path get
relationPath =  os.getcwd()
#dataPath = os.path.dirname(relationPath) + '/Data/web-Google.txt'
dataPath2 = 'C://Users//user//Desktop//BigDataHW5//web-Google.txt'
# read Data by spark context
data = sc.textFile(dataPath2)



#C://Users//user//Desktop//BigDataHW5//web-Google.txt


# In[3]:


# Q1

#initial setting 
sTime = time.time()

print("Start process Q1")

# map data to rdd by spark
rdd = data.map(lambda col:col.split()).map(lambda c:c[0])

# count each node out-degree, then sort key-value with ascending by out-degree
rd = sorted(rdd.countByValue().items(),key=lambda t:t[1], reverse=True)

# for convenient save result
cby = np.zeros((len(rd),2),int)

# for convenient save result
for i in range(len(rd)):
    cby[i][0] = rd[i][0]
    cby[i][1] = rd[i][1]
    
print("Q1 took: %.2fs" % (time.time()-sTime))


# save Q1 Result
sTime = time.time()
np.savetxt("Q1_Result.csv", cby, delimiter=",")
print("Save Q1 ans took %.2f" % (time.time()-sTime))


# In[6]:


# Q2

#initial setting 
sTime = time.time()


print("Start process Q2")

# map data to rdd by spark
rdd = data.map(lambda col:col.split()).map(lambda c:c[1])

# count each node out-degree, then sort key-value with ascending by out-degree
rd = sorted(rdd.countByValue().items(),key=lambda t:t[1], reverse=True)
    
print("Q2 took: %.2fs" % (time.time()-sTime))


# save Q2 Result
sTime = time.time()

# for convenient save result
cby = np.zeros((len(rd),2),int)

# for convenient save result
for i in range(len(rd)):
    cby[i][0] = rd[i][0]
    cby[i][1] = rd[i][1]
np.savetxt("Q2_Result.csv", cby, delimiter=",")
print("Save Q2 ans took %.2f" % (time.time()-sTime))


# In[ ]:


# Q3


#initial setting
sTime = time.time()
toNode = {}
fromNode = {}

print("Start process Q3")

# map data to rdd by spark
m = pd.read_csv(dataPath2, delimiter="\t", header=None, error_bad_lines=False)
length = len(d)

for mm in range(length):
    fromNode[mm] = []
    toNode[mm] = []

for i in range(length):
    fromNode[m[1][i]].append(m[0][i])
    toNode[m[0][i]].append(m[1][i])
    if i % 30000 == 0:
        print(i+1)
        
print("Q3 took: %.2fs" % (time.time()-sTime))

# save Q3 Result
sTime = time.time()
with open("Q3_Result.txt", 'w') as out:
    for i in range(length):
        out.write(str(i) + "\n")
        out.write(str(toNode[i]) + "\n")
        out.write(str(fromNode[i]) + "\n")

print("Save Q3 ans took %.2f" % (time.time()-sTime))


# In[ ]:




