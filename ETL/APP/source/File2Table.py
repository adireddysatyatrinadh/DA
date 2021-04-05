
import xml.etree.ElementTree as ET
from File import File
from Parameter import Parameter
from Date import Date
import sys
import traceback
import glob
import pathlib
import os
import codecs
import datetime

class File2Table:

    def __init__(self,pLog):
        self.log = pLog

    def CopyDataFromXMLFileToTable(self, prowjs,pwdb,pParameter):

        self.Parameter=pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10,lv_job_worker_last_run_start_dt,lv_job_last_success_run_dt,lv_job_group_cd,lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs
   
            iudic = {}  # INSERT/UPDATE Statement Bind Variable Values Dictionary

            if self.Parameter.getParameter("OS").lower() == "windows":
                xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "\\" + lv_job_program_arg2
            elif self.Parameter.getParameter("OS").lower() == "linux":
                xmlFileName = self.Parameter.getParameter('ETL_METADATA') + "/" + lv_job_program_arg2

            print(xmlFileName)
            f= File(self.log)
            xml=f.ReadXMLFile(xmlFileName)
            if (lv_job_group_cd=="JG_KIMS_FILE2DB_DA"):
                sdic={}
                sdic["SOURCE_SCHEMA_NAME"] = lv_job_program_arg6
                sdic["DESTINATION_SCHEMA_NAME"] = lv_job_program_arg7
            else:
                None

            # SELECT Statement Bind Variable Values Dictionary
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
                dtable_name=sdic["DESTINATION_SCHEMA_NAME"]+"."+i.attrib["NAME"]  # Destination Database Table Namess
                self.log.Write("Destination Table Name:", dtable_name)

            del sdic["SOURCE_SCHEMA_NAME"]
            del sdic["DESTINATION_SCHEMA_NAME"]

            usql += " UPDATE " + dtable_name + " SET "
            isql += " INSERT INTO " + dtable_name

            # self.log.Write("2")
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

            isql += "("+icol+")"+"VALUES"+"("+ival+")"

            usql += " WHERE 1=1 "


            #UPDATE Where clause
            key_columns = root.findall("./TABLE/KEY_COLUMNS/COLUMN")
            for i in key_columns:
                if self.Parameter.getParameter('destination_db.type').lower() == "oracle":
                    usql += " AND " + i.attrib["NAME"] + "=:" + i.attrib["NAME"]
                if self.Parameter.getParameter('destination_db.type').lower() == "postgresql":
                    usql += " AND " + i.attrib["NAME"] + "=%(" + i.attrib["NAME"]+")s"


            self.log.Write("\n","UPDATE STATEMENT:")
            self.log.Write(usql)
            self.log.Write("\n","INSERT STATEMENT:")
            self.log.Write(isql)

            if self.Parameter.getParameter("OS").lower() == "windows":
                inputFileNameFilter = self.Parameter.getParameter('ETL_INPUT') + "\\" + lv_job_loc_cd + "__" + lv_job_program_arg1 + "__" + "*.xml"
                successFolderName = self.Parameter.getParameter('ETL_INPUT') + "\\" + "success" + "\\"
            elif self.Parameter.getParameter("OS").lower() == "linux":
                inputFileNameFilter = self.Parameter.getParameter('ETL_INPUT') + "/" + lv_job_loc_cd + "__" + lv_job_program_arg1 + "__" + "*.xml"
                successFolderName=self.Parameter.getParameter('ETL_INPUT')+"/"+"success"+"/"

            for inputXMLFileName in glob.glob(inputFileNameFilter):
                try:
                    
                    dcur = pwdb.dcon.cursor()
                    pos=inputXMLFileName.index(".")
                    filenameprefix = inputXMLFileName[:pos]
                    filenamesuffix = inputXMLFileName[pos + 1:]

                    inputChecksumFileName=filenameprefix + ".checksum"
                    inputXMLBaseFileName = os.path.basename(inputXMLFileName)
                    inputChecksumBaseFileName = os.path.basename(inputChecksumFileName)


                    if not pathlib.Path(inputChecksumFileName).exists():
                        self.log.Write(inputChecksumFileName+" : Not Exists...")
                        self.log.Write(inputXMLFileName + " : Skiping...")
                        print(inputChecksumFileName+" : Not Exists...")
                        print(inputXMLFileName + " : Skiping...")

                        continue
                    with open(inputXMLFileName, 'r', encoding='utf-8') as xml_file:
                        tree = ET.parse(xml_file)
                    tree = ET.parse(inputXMLFileName)
                    table = tree.getroot()
                    for row in table:

                        iudic = {}
                        for column in row:
                            if column.text == "None":
                                iudic[column.tag] = None
                            else:
                                if column.tag.upper()=="DW_LAST_UPDATED_DT":
                                   iudic[column.tag]=datetime.datetime.now()
                                else:
                                   iudic[column.tag]=column.text

                        # self.log.Write(srow[columnIndex["STATECD"]],srow[columnIndex["STATENAME"]])
                        self.log.Write("\n","INSERT/UPDATE STATEMENT DICTIONARY:")
                        self.log.Write(iudic)
                        dcur.execute(usql,iudic)
                        self.log.Write("UPDATE ROWCOUNT=", dcur.rowcount)
                        if dcur.rowcount ==0:
                            dcur.execute(isql, iudic)
                            self.log.Write("INSERT ROWCOUNT=", dcur.rowcount)

                        pwdb.dcon.commit()
                    # END: element

                    # Move Success files to input\success folder
                    os.rename(inputXMLFileName,successFolderName+inputXMLBaseFileName)
                    os.rename(inputChecksumFileName, successFolderName + inputChecksumBaseFileName)
                    if dcur:
                        dcur.close()                    
                except Exception as e:
                    self.log.Write(e.__str__())
                    self.log.Write(traceback.print_exc())
                    self.log.Write("Error occured while loading : "+ inputXMLFileName)
                    self.log.Write(inputXMLFileName + " : Skiping...")
                    print(e)
                    print(traceback.print_exc())
                    print(traceback.format_exc())
                    print("Error occured while loading : "+ inputXMLFileName)
                    print(inputXMLFileName + " : Skiping...")
                    pwdb.dcon.rollback()
                    if dcur:
                        dcur.close()

                    None

            # END: name

            status=0
            errorCd=""
            errorMsg=""

            #dcur.close()

            return status,errorCd,errorMsg

        except BaseException as e:
            status=1
            errorCd=str(e)
            errorMsg=str(e)

            print(errorMsg)
            self.log.Write(traceback.print_exc())

            if dcur:
                dcur.close()
        finally:
            return status, errorCd, errorMsg
    #END: CopyDataFromXMLFileToTable
