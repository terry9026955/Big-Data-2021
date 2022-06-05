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
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions 
from pyspark.sql import Row
from collections import OrderedDict


# In[2]:


# 爬SGM檔案裡的資料

from bs4 import BeautifulSoup, SoupStrainer


def readData(fileName):
    data = ''
    f = open(fileName, 'rb') #也即 binary mode，read()操作返回的是bytes
    soup = BeautifulSoup(f, 'html.parser')
    
    # 取得body標籤內容
    contents = soup.find_all('body')
    return contents


# In[3]:


#問題一

sTime = time.time()
file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
bodyLen, shingleLen = 0, 0
#Shingle儲存
twoShingleSet = set()

#初始設定

print("Start count shingle")
for i in range(len(file)):
    fileName = "C://Users//user//Desktop//reuters21578//reut2-"
    fileName = fileName + file[i] + ".sgm"
    # 讀每個file中的body content
    data = readData(fileName)
    
    # 恢復所有 body content
    for content in data:
        if i < 2:
            bodyLen +=1
        # split one body content by \n
        for d in content.get_text().split("\n"):
            # remove header space and footer space
            sentence = d.strip()
            # regular expresisson
            pat = '[a-zA-Z]+'
            reContent = re.findall(pat, sentence)
            # select two shingle on each time
            for ind in range(len(reContent)-1):
                shingle = reContent[ind] + ' ' + reContent[ind+1]
                # add shingle
                twoShingleSet.add(shingle)
    print("file ", i, " done")
    
    
shingleLen = len(twoShingleSet)
print("Count shingle done\n")
print("Total body number: %d" % bodyLen)
print("Total shingle number: %d" % shingleLen)
print("Count shingle took: %.2fs" % (time.time()-sTime))


# In[4]:


#問題一

#初始設定

sTime = time.time()
file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
trueNum = 0
col = 0
pat = '[a-zA-Z]+'
matrix = np.full((shingleLen,bodyLen), False, dtype=bool)


print("Start calculate shingle matrix")
for i in range(2):
    fileName = "C://Users//user//Desktop//reuters21578//reut2-"
    fileName = fileName + file[i] + ".sgm"
    print(fileName +" start!")
    # read body content from each file
    data = readData(fileName)
    contInd = 0
     # save matrix result for each body
    for content in data:
        # save shingle from one body content
        bContShingleSet = set()
        # split one body content by \n
        for d in content.get_text().split("\n"):
            # remove header space and footer space
            sentence = d.strip()
            # regular expresisson
            reContent = re.findall(pat, sentence)
            # select two shingle on each time
            for r in range(len(reContent)-1):
                shingle = reContent[r] + ' ' + reContent[r+1]
                bContShingleSet.add(shingle)
        row = 0
        for t in twoShingleSet:
            if t in bContShingleSet:
                matrix[row][col] = True
                trueNum += 1
            row +=1
        col +=1
        contInd +=1
        if contInd % 100 ==0:
            print(contInd, " done!")

print("col :",col)
print("total trueNum ", trueNum)
print("Calculate shingle matrix done")
print("Calculate shingle matrix took %.2fs" % (time.time()-sTime))

#儲存問題一的內容      
sTime = time.time()
np.savetxt("Q1_Result.csv", matrix, delimiter=",")
print("Save shingle matrix took %.2f" % (time.time()-sTime))


# In[5]:


#問題二

#初始設定
sTime = time.time()
index = 0
flag = 0
minHash = np.zeros(bodyLen,int)
idx = np.full(shingleLen, False, dtype=bool)
curNum = 0
ind = 0
mod = 10

print("Start process minHash!")
while index < shingleLen and flag < bodyLen:
    curNum = ind
    index +=1
    if(curNum + mod)< shingleLen:
        ind = curNum + mod
    else:
        ind = (curNum + mod) % shingleLen
    while idx[ind] == True:
        ind += 1
    idx[ind] = True
    for c in range(bodyLen):
        if matrix[ind][c] == True:
            if minHash[c] == 0:
                minHash[c] = index
                flag +=1
    if index % 2000 ==0:
        print("flag ",flag, " done!")
        print("index ", index, " done!")
        
print("Process minHash done!")
print("MinHash calculate took %.2f" % (time.time()-sTime))

#儲存問題二的內容
sTime = time.time()
np.savetxt("Q2_Result.csv", minHash, delimiter=",")
print("Save minHash matrix took %.2fs" % (time.time()-sTime))


# In[ ]:




