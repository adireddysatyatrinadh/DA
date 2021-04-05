
import xml.etree.ElementTree as ET
from File import File
from Parameter import Parameter
from Date import Date
import sys
import traceback
import datetime

class Table2Table:

    def __init__(self,pLog):
        self.log = pLog

    def SyncDataFromTableToTable(self, prowjs,pwdb,pParameter):

        self.Parameter=pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10,lv_job_worker_last_run_start_dt,lv_job_last_success_run_dt,lv_job_group_cd,lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs
   
            iudic = {}  # INSERT/UPDATE Statement Bind Variable Values Dictionary

            xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "\\" + lv_job_program_arg2
            print(xmlFileName)
            f= File(self.log)
            xml=f.ReadXMLFile(xmlFileName)
            self.log.Write(xml)
            if (lv_job_group_cd=="JG_KIMS_DCDB2CDB_SAP"):
                sdic={}
            #    sdic["LAST_UPDATED_FROM_DT"]='1900-01-01'
            #    sdic["LAST_UPDATED_TO_DT"]='2020-11-30'
                sdic["LAST_UPDATED_FROM_DT"] = lv_job_last_success_run_dt
                sdic["LAST_UPDATED_TO_DT"] = lv_job_worker_last_run_start_dt
                sdic["PLANT_CODE"] = lv_job_program_arg5
                sdic["COSTCENTERCD"] = lv_job_program_arg10
                sdic["SOURCE_SCHEMA_NAME"] = lv_job_program_arg6
                sdic["DESTINATION_SCHEMA_NAME"] = lv_job_program_arg7
            elif (lv_job_group_cd=="JG_KIMS_DCDB2CDB_DA"):
                sdic={}
            #    sdic["LAST_UPDATED_FROM_DT"]='1900-01-01'
            #    sdic["LAST_UPDATED_TO_DT"]='2020-11-30'
                sdic["LAST_UPDATED_FROM_DT"] = lv_job_last_success_run_dt
                sdic["LAST_UPDATED_TO_DT"] = lv_job_worker_last_run_start_dt
                sdic["DW_FACILITY_CD"] = lv_job_program_arg5
                sdic["SOURCE_SCHEMA_NAME"] = lv_job_program_arg6
                sdic["DESTINATION_SCHEMA_NAME"] = lv_job_program_arg7
            elif (lv_job_group_cd=="JG_KIMS_DCDB2CDB_SAP_FIN"):
                sdic={}
            #    sdic["LAST_UPDATED_FROM_DT"]='1900-01-01'
            #    sdic["LAST_UPDATED_TO_DT"]='2020-11-30'
                sdic["LAST_UPDATED_FROM_DT"] = lv_job_last_success_run_dt
                sdic["LAST_UPDATED_TO_DT"] = lv_job_worker_last_run_start_dt
                sdic["BUSINESS_PLACE"] = lv_job_program_arg5
                sdic["PROFIT_CENTER"] = lv_job_program_arg10
                sdic["SOURCE_SCHEMA_NAME"] = lv_job_program_arg6
                sdic["DESTINATION_SCHEMA_NAME"] = lv_job_program_arg7
            else:
                None

            # SELECT Statement Bind Variable Values Dictionary
            ssql="" # SELECT SQL
            usql="" # UPDATE SQL
            isql="" # INSERT SQL
            icol="" # INSERT SQL Columns
            ival="" # INSERT SQL Values

            dsql=""
            root=ET.fromstring(xml)
            # self.log.Write(root.tag,root.attrib)
            table=root.findall("./TABLE")
            # self.log.Write("1")
            for i in table:
                #self.log.Write(i.attrib["NAME"])
                #self.log.Write(i.attrib["SOURCE_SCHEMA"])
                #self.log.Write(i.attrib["DESTINATION_SCHEMA"])
                stable_name=sdic["SOURCE_SCHEMA_NAME"]+"."+i.attrib["NAME"]  # Source Database Table Name
                dtable_name=sdic["DESTINATION_SCHEMA_NAME"]+"."+i.attrib["NAME"]  # Destination Database Table Namess
                self.log.Write("Source Table Name:", stable_name)
                self.log.Write("Destination Table Name:", dtable_name)

            del sdic["SOURCE_SCHEMA_NAME"]
            del sdic["DESTINATION_SCHEMA_NAME"]
            ssql += " SELECT "
            usql += " UPDATE " + dtable_name + " SET "
            isql += " INSERT INTO " + dtable_name

            # self.log.Write("2")
            # select columns
            ccount=0
            columns=root.findall("./TABLE/COLUMNS/COLUMN")
            self.log.Write(columns)
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
                if i == 0:
                    ssql += columnName[i]
                else:
                    ssql += "," + columnName[i]

                if i==columnCount-1:
                    if self.Parameter.getParameter('destination_db.type').lower()=="oracle":
                        usql += columnName[i] +" = :"+columnName[i]
                    if self.Parameter.getParameter('destination_db.type').lower()=="postgresql":
                        usql += columnName[i] +" = %("+columnName[i]+")s"

                    icol += columnName[i]

                    if self.Parameter.getParameter('destination_db.type').lower()=="oracle":
                        ival += ":"+columnName[i]
                    if self.Parameter.getParameter('destination_db.type').lower()=="postgresql":
                        ival += "%("+columnName[i]+")s"

                else:

                    if self.Parameter.getParameter('destination_db.type').lower()=="oracle":
                        usql += columnName[i] + " = :" + columnName[i] + ","
                    if self.Parameter.getParameter('destination_db.type').lower()=="postgresql":
                        usql += columnName[i] + " = %(" + columnName[i] +")s,"

                    icol += columnName[i] +","

                    if self.Parameter.getParameter('destination_db.type').lower()=="oracle":
                        ival += ":"+columnName[i]+","
                    if self.Parameter.getParameter('destination_db.type').lower()=="postgresql":
                        ival += "%(" + columnName[i] + ")s,"

            ssql += " FROM " + stable_name
            isql += "("+icol+")"+"VALUES"+"("+ival+")"

            ssql += " WHERE 1=1 "
            usql += " WHERE 1=1 "

            # self.log.Write("3")
            # SELECT Where clause
            filter_columns = root.findall("./TABLE/FILTER_COLUMNS/COLUMN")
            self.log.Write(filter_columns)
            index=0
            for i in filter_columns:
                if (index==0):

                    if self.Parameter.getParameter('source_db.type').lower()=="oracle":
                        ssql += " AND " + i.attrib["NAME"] + " BETWEEN " + ":LAST_UPDATED_FROM_DT AND :LAST_UPDATED_TO_DT"
                    if self.Parameter.getParameter('source_db.type').lower()=="postgresql":
                        ssql += " AND " + i.attrib["NAME"] + " BETWEEN " + "%(LAST_UPDATED_FROM_DT)s AND %(LAST_UPDATED_TO_DT)s"

                else:
                    if self.Parameter.getParameter('source_db.type').lower()=="oracle":
                        ssql += " AND " + i.attrib["NAME"] + "=:" + i.attrib["NAME"]
                    if self.Parameter.getParameter('source_db.type').lower()=="postgresql":
                        ssql += " AND " + i.attrib["NAME"] + "=%(" + i.attrib["NAME"]+")s"

                index+=1

            #if (self.Parameter.getParameter('source_db.type').lower() == "oracle"):
            #    ssql += " AND ROWNUM <=100"
                None
            #elif (self.Parameter.getParameter('source_db.type').lower() == "postgresql"):
            #    ssql += " LIMIT 100"
                None

            #UPDATE Where clause
            key_columns = root.findall("./TABLE/KEY_COLUMNS/COLUMN")
            self.log.Write(key_columns)
            for i in key_columns:
                if self.Parameter.getParameter('destination_db.type').lower() == "oracle":
                    usql += " AND " + i.attrib["NAME"] + "=:" + i.attrib["NAME"]
                if self.Parameter.getParameter('destination_db.type').lower() == "postgresql":
                    usql += " AND " + i.attrib["NAME"] + "=%(" + i.attrib["NAME"]+")s"

            self.log.Write("\n","SELECT STATEMENT:")
            self.log.Write(ssql)

            self.log.Write("\n","SELECT STATEMENT & DICTIONARY:")
            self.log.Write(ssql,sdic)
            scur = pwdb.scon.cursor()
            scur.execute(ssql,sdic)
            # srows=scur.fetchall()
            self.log.Write("rowcount=",scur.rowcount)

            self.log.Write("\n","UPDATE STATEMENT:")
            self.log.Write(usql)
            self.log.Write("\n","INSERT STATEMENT:")
            self.log.Write(isql)

            dcur = pwdb.dcon.cursor()
            # for srow in srows:
            while True:
                srow = scur.fetchone()
                if srow is None:
                    break

                iudic = {}
                index = 0
                # self.log.Write("Column Names:")
                for cname in columnIndex:
                    # self.log.Write(cname)
                    if cname == "DW_LAST_UPDATED_DT":
                        iudic[cname] = datetime.datetime.now()
                    else:
                        iudic[cname]=srow[columnIndex[cname]]
                    index+=1
                # self.log.Write(srow[columnIndex["STATECD"]],srow[columnIndex["STATENAME"]])
                #self.log.Write("\n","INSERT/UPDATE STATEMENT DICTIONARY:")
                #self.log.Write(iudic)
                dcur.execute(usql,iudic)
                self.log.Write("UPDATE ROWCOUNT=", dcur.rowcount)
                if dcur.rowcount ==0:
                    dcur.execute(isql, iudic)
                    self.log.Write("INSERT ROWCOUNT=", dcur.rowcount)

                pwdb.dcon.commit()

            # self.log.Write(5)

            status=0
            errorCd=""
            errorMsg=""

            scur.close()
            dcur.close()

            return status,errorCd,errorMsg

        except BaseException as e:

            status=1
            errorCd=str(e)
            errorMsg=str(e)

            print(errorMsg)
            self.log.Write(traceback.print_exc())

            if scur:
                scur.close()
            if dcur:
                dcur.close()
        finally:
            return status, errorCd, errorMsg
    #END: SyncDataFromTableToTable(self, prowjs,pwdb,Parameter)