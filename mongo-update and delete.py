import mongodb1 as gu
from pymongo import MongoClient,UpdateOne,UpdateMany
import pandas as pd
import numpy as np
from pandas import DataFrame
import json
import datetime

myclient = MongoClient('mongodb://localhost:27017')
mydb = myclient['repo']
mydb1 = myclient['master_collection']
mycol1 = mydb1['collection']
mycol = mydb['guid_repo']
mycol2 = mydb['golden_repo']

cur = mycol.find()
data_list = list(cur)
frame =(pd.DataFrame
        (data_list))
print(frame)

def writepath():
    a = mycol1.find({}, {'domain': 1, 'WBUCKETPATH': 1})
    path = []

    for data in a:
        c = data['WBUCKETPATH']
        path.append(str(c))
        path1 = ''.join(map(str, path))
        print(path1)
        return "YOUR_NEW_PATH"  # Change this to the desired path



w = writepath()
def writejson(writebucketpath):
     df = pd.DataFrame(data_list)
     df.to_json(writebucketpath,default_handler=str,orient='records')
     print('files written')
     return df

writejson(w)

class remove_unmerge:
    def __init__(self, id):
        self.id = id

    def unmerge_delete_guid(self):
        for item in self.id:
            query = {'oldguid': item}
            result = mycol.delete_many(query)
            print('records deleted for', item)

def unmerge_delete_guids():
    b = remove_unmerge(gu.b)
    b.unmerge_delete_guid()

unmerge_delete_guids()
