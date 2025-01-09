import json
# from datetime import datetime
# import time
import os
import sys
import glob
from elasticsearch import Elasticsearch, helpers
import pandas as pd
# import pdb
from icecream import ic
import metadata
from datetime import date



class stapsdown:

    def __init__(self, param_jason):
       print ("init")
       # self.es = Elasticsearch()
       with open(param_jason) as f:
          self.param_data = json.load(f)

       self.path = self.param_data["path"]
       self.pathlength = len(self.path)
       self.pathProcessed= self.param_data["pathProcessed"]
       self.index = self.param_data["index"]
       self.esServer = self.param_data["ESServer"]
       self.esUser = self.param_data["ESUser"]
       self.esPwd = self.param_data["ESPwd"]
       self.sqlite = self.param_data["sqlite"]
       # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))
       self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
       print ("After connection to ES")

       self.fullSQLMany=[]


       self.InProg = self.path + 'Staps_Enrich_In_Progress'
       if os.path.exists(self.InProg) == True :
          print ("Staps Process in Progress - Exiting" )
          exit(0)
       else:
          os.system('touch ' + self.InProg)

       self.myListCollectors = []
       self.csvFiles=[]
       self.DataFiles=[]


    # ---- getting Collectors MetaData
    def metadata(self):
        print("In MetaData")
        p1=metadata.MetaData(self.sqlite)

        # print ("In MetaData")
        # ---- Get A_IPs
        # print(myListIPs)
        self.myListIPs = p1.get_nodes()
        # breakpoint()
        # print(type(self.myListIPs), " -- " ,self.myListIPs)

        # ---- Get Collectors
        self.myListColls = p1.get_Colls()
        # print(type(self.myListIPs), " -- " ,self.myListIPs)

        return()


    def DataFile_List(self):
        print ("DataFile_List")
        self.csvFiles=glob.glob(self.path + "*STAP_STATUS*.csv")
        if len(self.csvFiles) == 0:
           print ("No File to Process")
           os.system('rm -f ' + self.path + 'Staps_Enrich_In_Progress')
           sys.exit()
        DataFile=[]
        for file in self.csvFiles:
          if "STAP" in file:
           COLL = file.split('/')
           COLL = file.split('/')[len(COLL)-1]
           # breakpoint()
           COLL = COLL.split('_')[1]
           print ("Collector : " , COLL)
           DataFile.append(COLL)
           DataFile.append(file)
           self.DataFiles.append(DataFile)
           DataFile=[]
           # print (self.DataFiles)

    def processOneFile(self,datafile):

      print ("processOneFile " , datafile)
      coll = datafile[0]
      df = pd.read_csv(datafile[1])
      df.rename(columns={ df.columns[0]: "UTC Offset" }, inplace = True)
      df = df.fillna("")
      self.field_list=df.columns

      for i in range(df.shape[0]):
         line = df.iloc[[i]]
         line['Collector'] = coll
         # breakpoint()
         count_doc = self.process_one_line(line)

      parsed = self.fullSQLMany

      return(count_doc)

    def into_ES(self, parsed):
      parsed_stap = []
      parsed_stap.append(parsed)
      # breakpoint()
      today=date.today()
      year = today.year
      month = today.month
      day = today.day
      try:
         response = helpers.bulk(self.es,parsed_stap, index=self.index+"-"+str(year)+"."+str(month)+"."+str(day))
         print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)
         pass

      return(len(parsed_stap))

    # ------ Process One Line ------
    def process_one_line(self,line):


         # Convert the DataFrame to a dictionary, using the 'records' orient
         lineDict = line.to_dict(orient='records')[0]

         item_count = 0


         # ---- Call to enrich 1 line ----
         line_meta = lineDict
         # line_meta = self.enrich_one_line(lineDict)
         line_meta = self.enrich_server(line_meta)
         line_meta = self.enrich_coll(line_meta)

         # breakpoint()

         p1.into_ES(line_meta)
         # self.fullSQLMany.append(line_meta)
         return()

    def enrich_server(self, lineDict):
        ic(lineDict['S-TAP Host'])
        server_metadata = self.myListIPs[self.myListIPs['Hostname']==lineDict['S-TAP Host']]
        # ic (server_metadata)
        if server_metadata.empty == True :
              # --- Return ---
              return(lineDict)
        else:
           server_metadata = server_metadata.to_dict(orient='records')[0]
           # -- Make it a Dict ---
           lineDict["Server Metadata"] = server_metadata

        return(lineDict)

    def enrich_coll(self, lineDict):
        coll_metadata = self.myListColls[self.myListColls['Colls']==lineDict['Collector']]
        if coll_metadata.empty == True :
              # --- Return ---
              return(lineDict)
        else:
           coll_metadata = coll_metadata.to_dict(orient='records')[0]
           # -- Make it a Dict ---
           lineDict["Collector Metadata"] = coll_metadata

        return(lineDict)


# --- Main  ---
if __name__ == '__main__':
    print("Start STAP Status Enrichment")

    p1 = stapsdown("param_data.json")

    p1.metadata()

    p1.DataFile_List()
    # --- Loop for each DAM data file
    doc_count_total = 0
    for datafile in p1.DataFiles:
        print('Processing : ',datafile)
        doc_count=p1.processOneFile(datafile)
        # move the processed file to Processed directory
        shortname=datafile[1][p1.pathlength:]
        # print ("Datafile",datafile," Short name= ",shortname)
        os.rename(datafile[1],p1.pathProcessed + shortname)
        # doc_count_total = doc_count_total + doc_count
    # print ('Nbr of Docs Processed' , doc_count_total)
    os.system('rm -f ' + p1.path + 'Staps_Enrich_In_Progress')
    print("End STAP Enrichment")
