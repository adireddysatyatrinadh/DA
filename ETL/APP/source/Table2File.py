from File import File
from DB import DB
from Parameter import Parameter
import xml.etree.ElementTree as ET
import sys
import datetime
from Date import Date
import traceback

class Table2File:

    def __init__(self,pLog):
        self.log=pLog

    def CopyDataFromTableToXMLFile(self, prowjs,pwdb,pParameter):

        self.Parameter = pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10, lv_job_worker_last_run_start_dt, lv_job_last_success_run_dt, lv_job_group_cd, lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs

            xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "\\" + lv_job_program_arg2
            print(xmlFileName)
            f = File(self.log)
            xml = f.ReadXMLFile(xmlFileName)

            # Output FileName
            fdt = lv_job_last_success_run_dt.strftime('%Y%m%d%H%M%S')
            tdt = lv_job_worker_last_run_start_dt.strftime('%Y%m%d%H%M%S')
            outputFileName = self.Parameter.getParameter('ETL_OUTPUT') + "\\" + lv_job_loc_cd + "__" + lv_job_program_arg1 + "__" + fdt + "-" + tdt + ".xml"
            outputControlFileName = self.Parameter.getParameter('ETL_OUTPUT') + "\\" + lv_job_loc_cd + "__" + lv_job_program_arg1 + "__" + fdt + "-" + tdt + ".checksum"
            self.log.Write("OutputFileName=", outputFileName)
            print("OutputFileName=", outputFileName)

            if (lv_job_group_cd=="JG_KIMS_CDB2FILE_DA"):
                sdic = {}
                sdic["LAST_UPDATED_FROM_DT"] = lv_job_last_success_run_dt
                sdic["LAST_UPDATED_TO_DT"] = lv_job_worker_last_run_start_dt
                sdic["DW_FACILITY_CD"] = lv_job_program_arg5
                sdic["SCHEMA_NAME"] = lv_job_program_arg6
            else:
                None

            sql="" # SELECT SQL
            root=ET.fromstring(xml)
            #self.log.Write(root.tag,root.attrib)
            table=root.findall("./TABLE")
            #self.log.Write("1")
            for i in table:
                #self.log.Write(i.attrib["NAME"])
                #self.log.Write(i.attrib["SOURCE_SCHEMA"])
                #self.log.Write(i.attrib["DESTINATION_SCHEMA"])
                table_name=sdic["SCHEMA_NAME"]+"."+i.attrib["NAME"] # Database Table Name
                self.log.Write("Table Name:",table_name)

            del sdic["SCHEMA_NAME"]

            sql += " SELECT "

            #self.log.Write("2")
            # select columns
            ccount=0
            columns=root.findall("./TABLE/COLUMNS/COLUMN")
            # ColumnNames Dic
            columnName={}
            columnType={}
            columnIndex={}
            columnCount=0
            for i in columns:
                #self.log.Write(i.attrib["NAME"],i.attrib["DATA_TYPE"])
                columnName[columnCount]=i.attrib["NAME"]
                columnType[columnCount]=i.attrib["DATA_TYPE"]
                columnIndex[i.attrib["NAME"]]=columnCount
                columnCount+=1

            for i in range(0,columnCount):
                if i==0:
                    sql += columnName[i]
                else:
                    sql += "," + columnName[i]

            sql += " FROM " + table_name
            sql += " WHERE 1=1 "

            #self.log.Write("3")
            #SELECT Where clause
            filter_columns = root.findall("./TABLE/FILTER_COLUMNS/COLUMN")
            index=0
            for i in filter_columns:
                if (index==0):

                    if (self.Parameter.getParameter('source_db.type').lower()=="oracle"):
                        sql += " AND " + i.attrib["NAME"] + " BETWEEN " + ":LAST_UPDATED_FROM_DT AND :LAST_UPDATED_TO_DT"
                    if (self.Parameter.getParameter('source_db.type').lower()=="postgresql"):
                        sql += " AND " + i.attrib["NAME"] + " BETWEEN " + "%(LAST_UPDATED_FROM_DT)s AND %(LAST_UPDATED_TO_DT)s"

                else:
                    if (self.Parameter.getParameter('source_db.type').lower()=="oracle"):
                        sql += " AND " + i.attrib["NAME"] + "=:" + i.attrib["NAME"]
                    if (self.Parameter.getParameter('source_db.type').lower()=="postgresql"):
                        sql += " AND " + i.attrib["NAME"] + "=%(" + i.attrib["NAME"]+")s"

                index+=1


            if (self.Parameter.getParameter('source_db.type').lower() == "oracle"):
                sql += " AND ROWNUM <=100"
                None
            elif (self.Parameter.getParameter('source_db.type').lower() == "postgresql"):
                sql += " LIMIT 100"
                None

            self.log.Write("\n","SELECT STATEMENT:")
            self.log.Write(sql)

            self.log.Write("\n","SELECT STATEMENT & DICTIONARY:")
            self.log.Write(sql,sdic)
            self.scur=pwdb.scon.cursor()
            self.scur.execute(sql,sdic)
            #srows=self.scur.fetchall()
            self.log.Write("rowcount=",self.scur.rowcount)

            #self.log.Write(srows)
            #self.log.Write(4)

            #ColumnName ColumnIndex Mapping
            #columnIndex=self.columnIndexNameMap(self.scur)

            outputFileObj = open(outputFileName, "w",encoding="utf-8")
            outputFileObj.write('<TABLE>\n')

            #for srow in srows:
            while True:
                row = self.scur.fetchone()
                if row is None:
                    break

                outputFileObj.write('<ROW>\n')
                for i in range(0, columnCount):
                    outputFileObj.write("<"+columnName[i]+">"+self.Encode(str(row[i]))+"</"+columnName[i]+">\n")
                outputFileObj.write('</ROW>\n')

            outputFileObj.write('</TABLE>\n')
            outputFileObj.close()

            outputControlFileObj = open(outputControlFileName, "w")
            outputControlFileObj.close()

            status=0
            errorCd=""
            errorMsg=""

            self.scur.close()

            return status,errorCd,errorMsg

        except BaseException as e:
            status=1
            errorCd=str(e)
            errorMsg=str(e)
            #print(str(traceback.print_exc()))
            self.log.Write(traceback.print_exc())


            if self.scur:
                self.scur.close()
            if outputFileObj:
                outputFileObj.close()
            if outputControlFileObj:
                outputControlFileObj.close()

        finally:
            return status, errorCd, errorMsg
    #END: SyncTable(self,xml,sdic)

    def Encode(self,pText):
            pText= pText.replace("&","&amp;")
            pText= pText.replace("<","&lt;")
            pText= pText.replace(">","&gt;")
            return pText