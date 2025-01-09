import json
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import numpy as np
import glob
import os
import sys
from datetime import datetime
import time
from icecream import ic

class stapsdown:

  def __init__(self,param_json):

     # Initalization of variables
     self.myListMeta_IP = []
     self.myListMeta_Env = []
     self.myListMeta_SubEnv = []
     self.myListMeta_FQDN = []
     self.myListIPs = []
     self.cnt = 0
     self.DataFiles = []
     self.df_colls = []
     self.df_staps = []


     # Access to SQLite
     self.sqlite_connect = None
     self.cursor = None

     self.CURRENT_TIMESTAMP = None
     self.myListColls = []

     # Get the param file
     with open(param_json) as f:
          self.param_data = json.load(f)

     # Access to ElasticSearch local
     self.path = self.param_data["path"]
     self.pathlength = len(self.path)
     self.pathProcessed= self.param_data["pathProcessed"]
     self.index = self.param_data["index"]
     self.esServer = self.param_data["ESServer"]
     self.esUser = self.param_data["ESUser"]
     self.esPwd = self.param_data["ESPwd"]
     self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
     # self.collHost = self.param_data["HostnameColl"]
     # self.collIP = self.param_data["IPColl"]
     # self.stapHost = self.param_data["HostnameStap"]
     # self.stapIP = self.param_data["IPStap"]

     print ("After connection to ES")


     self.Docs2F = None

  def collstap2(self) :

     # generation of (stapip, collip, last_contact, primcollip)
     # read ct22-enrich-staps
     query = {
           "query": {
                "bool": {
                     "must": [
                           {
                            "match_all": {}
                           },
                           {
                           "range": {
                               "Timestamp": {
                                   "lte": '2021-05-27T08:00:00Z'
                                }
                             }
                           }
                            ]
                       }
                    },
               "_source": ["Collector", "S-TAP Host", "Last Response Received"]
              }

     results = self.es.search(index='ct22-enrich_staps', body=query)

     print(results)

     return()

  def collstap(self) :
      query = {
         "query": {
            "match_all": {}
             },
         # "_source": ["title", "author"]
               "_source": ["Collector", "S-TAP Host" , "Last Response Received"]
               , "size" : 1000
        }

      results = self.es.search(index='ct22-enrich_staps', body=query)

      data = pd.DataFrame.from_dict(results['hits']['hits'])

      list_stap_coll = data['_source'].tolist()
      # ic(list_stap_coll)
      self.df_stap_coll = pd.DataFrame.from_dict(list_stap_coll)
      self.df_stap_coll['Last Response Received'] = pd.to_datetime(self.df_stap_coll['Last Response Received'])
      ic(self.df_stap_coll)

      # pivot_table = self.df_stap_coll.pivot_table(values='Last Response Received', index=['Collector','S-TAP Host'] , aggfunc=np.max, columns=['Collector','S-TAP Host'])
      pivot_table = self.df_stap_coll.pivot_table(values='Last Response Received', index=['Collector','S-TAP Host'] , aggfunc=np.max)


      # for i in range(df.shape[0]):
      parsed = []
      for i in range(pivot_table.shape[0]):
         # line = df.iloc[[i]]
         line = pivot_table.iloc[[i]]
         # print(line)
         line = line.reset_index()
         dict = line.to_dict(orient='records')
         #dict = line.to_dict(orient='index')
         parsed.append(dict[0])
         # parsed.append(dict)

      # breakpoint()

      # print (pivot_table)
      # print (type(pivot_table))
      # print (len(pivot_table))

      # Convert the DataFrame to a dictionary, using the 'records' orient
      # Dict = pivot_table.to_dict(orient='records')
      # breakpoint()
      p1.purge_ES()
      count = p1.into_ES(parsed)

      # breakpoint()

      return()

  def into_ES(self, parsed):
      # parsed_stap = []
      # parsed_stap.append(parsed)
      # breakpoint()
      try:
         response = helpers.bulk(self.es,parsed, index='ct22_staps_down')
         print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)
         pass

      # return(response)
      return()

  def purge_ES(self) :
      query = {
         "query": {
            "match_all": {}
             }
        }

      results = self.es.delete_by_query(index='ct22_staps_down', body=query)

# --- Main  ---
if __name__ == '__main__':
    print("Start STAP Downs Detection")

    p1 = stapsdown("param_data.json")

    p1.collstap()

    # doc_count_total = doc_count_total + doc_count
    # print ('Nbr of Docs Processed' , doc_count_total)
    os.system('rm -f ' + p1.path + 'Staps_Enrich_In_Progress')
    print("End STAP Down detection")

