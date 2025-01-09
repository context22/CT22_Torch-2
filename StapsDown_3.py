import json
from elasticsearch import Elasticsearch, helpers
import pandas as pd
from icecream import ic
from datetime import timedelta
import pdb


class stapsoutage:

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
     self.days = self.param_data["stapOutage"]["days"]
     self.hours = self.param_data["stapOutage"]["hours"]
     # self.collIP = self.param_data["IPColl"]
     # self.stapHost = self.param_data["HostnameStap"]
     # self.stapIP = self.param_data["IPStap"]

     # print (self.days)
     print ("After connection to ES")
     # exit(0)

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
               "_source": ["S-TAP Host" , "Last Response Received"]
               , "size" : 1000
        }

      index_pattern = "ct22-enrich_staps*"

      results = self.es.search(index=index_pattern, body=query)

      data = pd.DataFrame.from_dict(results['hits']['hits'])

      list_stap_coll = data['_source'].tolist()
      # ic(list_stap_coll)
      self.df_stap_coll = pd.DataFrame.from_dict(list_stap_coll)
      self.df_stap_coll['Last Response Received'] = pd.to_datetime(self.df_stap_coll['Last Response Received'])
      # ic(self.df_stap_coll)
      # Sort by values in a specific column
      self.df_stap_coll.sort_values(by=["S-TAP Host" ,'Last Response Received'], ascending=False, inplace=True)
      self.df_stap_coll = self.df_stap_coll.reset_index()
      # ic(self.df_stap_coll)

      # for i in range(df.shape[0]):
      outage_item = []
      outage_list = []
      previous_stap = pd.Series('q')
      previous_LRR = pd.Series()
      # compteur = 0

      for i in range(self.df_stap_coll.shape[0]):
         # line = df.iloc[[i]]
         # breakpoint()
         line = self.df_stap_coll.iloc[[i]]

         # ic(type(line))
         # ic(line['S-TAP Host'].iloc[[0]])
         # current_stap = str(line['S-TAP Host'].item())
         current_stap = line['S-TAP Host']
         # ic(type(current_stap), current_stap)
         # ic(line['Last Response Received'])
         # ic(current_stap.index, previous_stap.index)
         previous_stap.index = current_stap.index
         if current_stap.equals(previous_stap)  :
             # date_format = '%Y-%m-%d %H:%M:%S%z'
             # date_obj_current = datetime.strptime(line['Last Response Received'], date_format)
             current_LRR = line['Last Response Received']
             #date_obj_previous = datetime.strptime(previous_LRR, date_format)
             previous_LRR.index = current_LRR.index
             outage = previous_LRR - current_LRR
             # ic(type(outage))
             # ic(outage)
             td = timedelta(days=self.days, hours=self.hours)
             # ic(type(td))
             # ic(td)
             outage_value = outage.iloc[0]  # Extract the first (and only) value
             # ic(outage_value)  # Output: 3 days 06:00:00
             # compteur = compteur + 1
             # print ("Before test on Outage", compteur)
             if pd.to_timedelta(outage_value) > td:
                 print("Outage is greater than td.")
                 stap = line['S-TAP Host'].item()
                 start_outage = line['Last Response Received'].item()
                 end_outage = previous_LRR.item()
                 dict_outage_item = {'S-TAP Host': stap, 'Start Outage': start_outage, 'End_Outage': end_outage}  # Valid dictionary

                 # ic(outage_item)
                 outage_list.append(dict_outage_item)

         # line = line.reset_index()
         # dict = line.to_dict(orient='index')
         # parsed.append(dict[0])
         # parsed.append(dict)
         previous_stap = line['S-TAP Host']
         previous_LRR = line['Last Response Received']

      # breakpoint()

      p1.purge_ES()
      ic(outage_list)
      count = p1.into_ES(outage_list)

      return()

  def into_ES(self, parsed):
      breakpoint()
      try:
         response = helpers.bulk(self.es,parsed, index='ct22_staps_outage')
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
      try :
        results = self.es.delete_by_query(index='ct22_staps_outage', body=query)
      except Exception as e:
          pass
      return()

# --- Main  ---
if __name__ == '__main__':
    print("Start STAP Outage Detection")

    p1 = stapsoutage("param_data.json")

    p1.collstap()

    print("End STAP Outage detection")

