########################################################################################
#  Date         Author          Change_Description
########################################################################################
#  18-Feb-2021  AST             Created
#  19-Feb-2021  Sruthi
########################################################################################
from Parameter import Parameter
import xml.etree.ElementTree as ET
from File import File
import traceback
import sys
from Date import Date
import datetime

class Table2Index:

    def __init__(self,pLog):
        self.log=pLog

    def SyncDataFromTableToIndex(self, prowjs,plogdb,pwdb,pParameter):

        self.Parameter = pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10, lv_job_worker_last_run_start_dt, lv_job_last_success_run_dt, lv_job_group_cd, lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs

            #logdbcon = plogdb.GetLogDBConnection()
            #logdbcur = logdbcon.cursor()

            if self.Parameter.getParameter('etl_db.type').lower() == "oracle":
                sql = ""
                sql += " SELECT "
                sql += " g.grp_short_name"
                sql += " FROM dl_group g"
                sql += " WHERE g.grp_cd = :grp_cd "
                sql += " AND g.record_status = 'A'"
            elif self.Parameter.getParameter('etl_db.type').lower() == "postgresql":
                sql = ""
                sql += " SELECT "
                sql += " g.grp_short_name"
                sql += " FROM dl_group g"
                sql += " WHERE g.grp_cd = %(grp_cd)s "
                sql += " AND g.record_status = 'A'"

            ldic = {}
            ldic['grp_cd'] = lv_job_grp_cd
            self.log.Write(sql, ldic)
            #logdbcur.execute(sql, ldic)
            #self.log.Write("rowcount=", logdbcur.rowcount)
            #lv_grp_short_name, = logdbcur.fetchone()
            #logdbcur.close()
            print(lv_job_cd,lv_job_step_cd)


            if self.Parameter.getParameter("os").lower() == "windows":
                xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "\\" + lv_job_program_arg2
            elif self.Parameter.getParameter("os").lower() == "linux":
                xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "/" + lv_job_program_arg2

            print(xmlFileName)
            f = File(self.log)
            xml = f.ReadXMLFile(xmlFileName)

            if (lv_job_group_cd=="JG_KIMS_PGSQL2ES_DA"):
                dic = {}
                #    sdic["LAST_UPDATED_FROM_DT"]='1900-01-01'
                #    sdic["LAST_UPDATED_TO_DT"]='2020-11-30'
                dic["LAST_UPDATED_FROM_DT"] = lv_job_last_success_run_dt
                dic["LAST_UPDATED_TO_DT"] = lv_job_worker_last_run_start_dt
                dic["LOC_CD"] = lv_job_program_arg10
                dic["SCHEMA_NAME"] = lv_job_program_arg6
            else:
                None

            # self.log.Write(status,errorCd,errorMsg)
            # return status,errorCd,errorMsg

            sql="" # SELECT SQL
            sql += " SELECT "

            root=ET.fromstring(xml)
            #self.log.Write(root.tag,root.attrib)
            table=root.findall("./TABLE")
            #self.log.Write("1")
            for i in table:
                #self.log.Write(i.attrib["NAME"])
                #self.log.Write(i.attrib["SOURCE_SCHEMA"])
                #self.log.Write(i.attrib["DESTINATION_SCHEMA"])
                table_name = dic["SCHEMA_NAME"]+"." + i.attrib["NAME"] # Database Table Name
                index_name = lv_job_grp_cd + "_" + i.attrib["INDEX_NAME"]
                self.log.Write("Table Name:",table_name)
                self.log.Write("Index Name:",index_name)
            #self.log.Write("2")

            #  Read Table Column Names and Data Types
            #ccount=0
            columns=root.findall("./DB/COLUMNS/COLUMN")
            # ColumnNames Dic
            columnName = {}
            columnType = {}
            columnIndex = {}
            columnCount = 0
            for i in columns:
                #self.log.Write(i.attrib["NAME"],i.attrib["DATA_TYPE"])
                columnName[columnCount]=i.attrib["NAME"]
                columnType[columnCount]=i.attrib["DATA_TYPE"]
                columnIndex[i.attrib["NAME"]]=columnCount
                columnCount += 1

            #  Read Index Column Names and Data Types
            #iccount = 0
            icolumns = root.findall("./ES/COLUMNS/COLUMN")
            # iColumnNames Dic
            icolumnName = {}
            icolumnType = {}
            icolumnIndex = {}
            icolumnCount = 0
            for i in icolumns:
                # self.log.Write(i.attrib["NAME"],i.attrib["DATA_TYPE"])
                icolumnName[icolumnCount] = i.attrib["NAME"]
                icolumnType[icolumnCount] = i.attrib["DATA_TYPE"]
                icolumnIndex[i.attrib["NAME"]] = icolumnCount
                icolumnCount += 1


            for i in range(0,columnCount):
                if i == 0:
                    sql += columnName[i]
                else:
                    sql += "," + columnName[i]

            sql += " FROM " + table_name
            sql += " WHERE 1=1 "

            #self.log.Write("3")
            #SELECT Where clause
            filter_columns = root.findall("./DB/FILTER_COLUMNS/COLUMN")
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

            #if (self.Parameter.getParameter('source_db.type').lower() == "oracle"):
            #    sql += " AND ROWNUM <=1000"
            #    None
            #elif (self.Parameter.getParameter('source_db.type').lower() == "postgresql"):
            #    sql += " LIMIT 1000"
            #    None


            self.log.Write("\n","SELECT STATEMENT:")
            self.log.Write(sql)

            id_column = root.findall("./ES/ID_COLUMN/COLUMN")
            index = 0
            for i in id_column:
                id_column_name = i.attrib["NAME"]

            self.log.Write("\n","ID COLUMN NAME:")
            self.log.Write(id_column_name)

            self.log.Write("\n", "SELECT STATEMENT & DICTIONARY:")
            self.log.Write(sql, dic)
            scur = pwdb.scon.cursor()
            scur.execute(sql, dic)
            self.log.Write("rowcount=",scur.rowcount)

            while True:
                row = scur.fetchone()
                if row is None:
                    break

                dic = {}
                for i in range(0, icolumnCount):
                    if columnName[i].upper() == "DW_LAST_UPDATED_DT":
                       dic[icolumnName[i]] = datetime.datetime.now()
                    else:
                       dic[icolumnName[i]] = row[i]

                id_column_value = row[columnIndex[id_column_name]]
                res = pwdb.dcon.update(index=index_name,doc_type='_doc',id=id_column_value,
                   body={'doc':dic,'doc_as_upsert':True})
#index(index=index_name, id=id_column_value, body=dic)

            status = 0
            errorCd = ""
            errorMsg = ""

            scur.close()

            return status, errorCd, errorMsg

        except BaseException as e:
            status = 1
            errorCd = str(e)
            errorMsg = str(e)

            self.log.Write(traceback.print_exc())

            if scur:
                scur.close()

        finally:
            return status, errorCd, errorMsg
    #END: SyncDataFromTableToIndex()
#END: DB2ESSync
