import pymongo
from pymongo import MongoClient
import pandas as pd
from pandas import DataFrame
import numpy as np
import json
import datetime

myclient = MongoClient('mongodb://localhost:27017/')
mydb = myclient['repo']
mydb1 = myclient['master_collection']
mycol1 = mydb1['collection']
mycol = mydb['guid_repo']
mycol2 = mydb['golden_repo']

cur = mycol.find()
cur_list = list(cur)
pd_cur = pd.DataFrame(cur_list)
#print(pd_cur)

def getfilepath():
    a = mycol1.find({},{'domain_id':1,'IBUCKETPATH':1})
    path = []
    for data in a:
        c = data['IBUCKETPATH']
        path.append(str(c))
        path1 = ''.join(map(str,path))
        print(path1)
        return path1

a = getfilepath()

def readdata(a):
    data = open(a,'r')
    input = pd.read_csv(data)
    data_select = input['guid'].tolist()
    data2 = pd.DataFrame(cur_list)
    data_find = data2['GUID'].tolist()
    c = [int(x) for x in data_find]
    merge_guid = []
    for i in data_select:
        if i in c:
            print('Merge records',i)
            merge_guid.append(str(i))
        else:
            print('Records which does not match',i)
    return merge_guid

b = readdata(a)

class unmerge:
    def _init_(self,id):
        self.id = id

    def unmerge_golden(self):
        for item in self.id:
            a,b = {'GUID':item},{'$set':{'unmerge':'yes'}}
            mycol2.update_many(a,b)
            print('merge the records',item)

    def remove_const(self):
        for item in self.id:
            a,b = {'GUID':item},{'$unset':{'Consolidation_Ind':''}}
            mycol.update_many(a,b)
            print('data has been updated',item)

    def add_oldguid(self):
         for item in self.id:
             a,b = {'GUID':item},{'$set':{'oldguid':item}}
             mycol.update_many(a,b)
             print('old guid has been updated in mongodb',item)

    def remove_guid(self):
        for item in self.id:
            a,b = {'GUID':item},{'$set':{'GUID':''}}
            mycol.update_many(a,b)
            print('data has been set')


def main():
    try:
        b = unmerge(readdata(getfilepath()))
        b.unmerge_golden()
        b.remove_const()
        b.add_oldguid()
        b.remove_guid()
        print('function executed')
    except Exception as e:
        print(f'function not executing due to exception:{e}')

main()
