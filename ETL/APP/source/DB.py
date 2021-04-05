import traceback
from Parameter import Parameter
import json
import os
import requests

if Parameter.getETLParameter('source_db.type').lower() == "oracle" or Parameter.getETLParameter('destination_db.type').lower() == "oracle" or Parameter.getETLParameter('etl_db.type').lower() == "oracle":
    import cx_Oracle

if Parameter.getETLParameter('source_db.type').lower() == "postgresql" or Parameter.getETLParameter('destination_db.type').lower() == "postgresql" or Parameter.getETLParameter('etl_db.type').lower() == "postgresql":
    import psycopg2

if Parameter.getETLParameter('source_db.type').lower() == "elasticsearch" or Parameter.getETLParameter('destination_db.type').lower() == "elasticsearch" or Parameter.getETLParameter('etl_db.type').lower() == "elasticsearch":
    from elasticsearch import Elasticsearch

if Parameter.getETLParameter('source_server.type').lower() == "flask" or Parameter.getETLParameter('destination_server.type').lower() == "flask":
    import flask

class DB:


    def __init__(self,pParameter,pLog):
        self.log=pLog

        self.log.Write("BEGIN: DB Constructor")

        self.scon=None
        self.dcon=None

        self.ssrv=None
        self.dsrv=None

        self.Parameter=pParameter

        self.log.Write("END: DB Constructor")

    def GetDestinationDBConnection(self):
        # Destination Database Connection
        return self.dcon

    def OpenDestinationDBConnection(self):
        if self.dcon:
            return self.dcon

        self.dcon=None

        self.log.Write("BEGIN: OpenDestinationDBConnection")

        # Destination Database Connection
        if self.Parameter.getParameter('destination_db.type').lower() == "oracle":
            self.dcon = cx_Oracle.connect(
            self.Parameter.getParameter('destination_db.username'),
            self.Parameter.getParameter('destination_db.password'),
            self.Parameter.getParameter('destination_db.host') + ":" + self.Parameter.getParameter('destination_db.port') + "/" + self.Parameter.getParameter('destination_db.name'),
            encoding=self.Parameter.getParameter('destination_db.encoding'))
            self.log.Write("Destination DB Connection: Opened")

        if self.Parameter.getParameter('destination_db.type').lower() == "postgresql":
            constring = ""
            constring += "dbname="+self.Parameter.getParameter('destination_db.name')+" "
            constring += "user="+self.Parameter.getParameter('destination_db.username')+" "
            constring += "host="+self.Parameter.getParameter('destination_db.host')+" "
            constring += "port="+self.Parameter.getParameter('destination_db.port')+" "
            constring += "password="+self.Parameter.getParameter('destination_db.password')+" "

            self.dcon = psycopg2.connect(constring)


            self.log.Write("Destination DB Connection: Opened")
            self.log.Write("END: OpenDestinationDBConnection")

        if self.Parameter.getParameter('destination_db.type').lower() == "elasticsearch":
            # constring = "http://"
            # constring += self.Parameter.getParameter('destination_db.username') + ":"
            # constring += self.Parameter.getParameter('destination_db.password') + "@"
            # constring += self.Parameter.getParameter('destination_db.host') + ":"
            # constring += self.Parameter.getParameter('destination_db.port')

            # constringlist=[constring]
            # self.dcon = Elasticsearch(constringlist)
            # a=self.Parameter.getParameter('destination_db.host')
            # b=self.Parameter.getParameter('destination_db.port')
            # c=self.Parameter.getParameter('destination_db.username')
            # d=self.Parameter.getParameter('destination_db.password')
            # self.dcon = Elasticsearch([{'host':a,'port':b }],
            # http_auth=( c+':'+d ),
            # timeout=None,maxsize=1000,block=True)
            ###print(dcon)
            self.dcon = Elasticsearch([{'host': 'localhost', 'port': 9200}],
                                      http_auth=('elastic' + ':' + 'Suresh@suvarna$123#Analytics'),
                                      timeout=None, maxsize=1000, block=True)
            ###print(dcon)
            # return self.dcon
            return self.dcon
            self.log.Write("Destination DB Connection: Opened")
            self.log.Write("END: OpenDestinationDBConnection")

        return self.dcon

    def CloseDestinationDBConnection(self):
        # Destination Database Connection

        if Parameter.getETLParameter('destination_db.type').lower() == "elasticsearch":
            return None

        if  self.dcon:
            self.dcon.close()

    def GetSourceDBConnection(self):
        # Destination Database Connection
        return self.scon



    def OpenSourceDBConnection(self):
        if self.scon:
            return self.scon

        self.scon=None
        # Source Database Connection
        self.log.Write("BEGIN: OpenSourceDBConnection")
        if self.Parameter.getParameter('source_db.type').lower() == "oracle":
            self.scon = cx_Oracle.connect(
                self.Parameter.getParameter('source_db.username'),
                self.Parameter.getParameter('source_db.password'),
                self.Parameter.getParameter('source_db.host') + ":" + self.Parameter.getParameter(
                    'source_db.port') + "/" + self.Parameter.getParameter('source_db.name'),
                encoding=self.Parameter.getParameter('source_db.encoding'))
            self.log.Write("Source DB Connection: Opened")
            self.log.Write("END: OpenSourceDBConnection")

        if self.Parameter.getParameter('source_db.type').lower() == "postgresql":

            constring = ""
            constring += "dbname=" + self.Parameter.getParameter('source_db.name') + " "
            constring += "user=" + self.Parameter.getParameter('source_db.username') + " "
            constring += "host=" + self.Parameter.getParameter('source_db.host') + " "
            constring += "port=" + self.Parameter.getParameter('source_db.port') + " "
            constring += "password=" + self.Parameter.getParameter('source_db.password') + " "

            self.scon = psycopg2.connect(constring)

            self.log.Write("Source DB Connection: Opened")
            self.log.Write("END: OpenSourceDBConnection")

        if self.Parameter.getParameter('source_db.type').lower() == "elasticsearch":
            constring = "http://"
            constring += self.Parameter.getParameter('source_db.username') + ":"
            constring += self.Parameter.getParameter('source_db.password') + "@"
            constring += self.Parameter.getParameter('source_db.host') + ":"
            constring += self.Parameter.getParameter('source_db.port')

            self.scon = Elastisearch(constring)

            self.log.Write("Source DB Connection: Opened")
            self.log.Write("END: OpenSourceDBConnection")

        return self.scon

    def CloseSourceDBConnection(self):
        # Close Source Database Connection
        if Parameter.getETLParameter('source_db.type').lower() == "elasticsearch":
            return None

        if self.scon:
            self.scon.close()

    def GetDestinationServerToken(self):
        # Destination Database Connection
        return self.dsrv["token"]

    def AuthenticateDestinationServer(self):

        # Authenticate
        self.log.Write("BEGIN: AuthenticateDestincationServer")

        response_dic={}
        # Destination Server Authentication
        if self.Parameter.getParameter('destination_server.type').lower() == "flask":
            url=self.Parameter.getParameter('destination_server.host')+"/"+"login"
            auth = (self.Parameter.getParameter('destination_server.username'), self.Parameter.getParameter('destination_server.password'))
            print(url)
            print(auth)
            response = requests.post(url, auth=auth)

            if response.status_code == 200:
                response_dic = json.loads(response.text)
                response_dic["access_url"]=self.Parameter.getParameter('destination_server.host')+"/file/write"
                access_token = response_dic["access_token"]
                self.log.Write("access_token=",access_token)
                print("access_token=",access_token)


            else:
                self.log.Write("AuthenticateDestincationServer: Failed")
                self.log.Write(response.text)
                print(response.text)
                exit(0)

            self.log.Write("AuthenticateDestincationServer: Success")

        self.log.Write("END: AuthenticateDestincationServer")

        self.dsrv=response_dic

        return self.dsrv
