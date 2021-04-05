from File import File
from Parameter import Parameter
from Date import Date
import sys
import traceback
import glob
import pathlib
import os
import requests
import base64
import json


class File2File:

    def __init__(self,pLog):
        self.log = pLog

    def CopyFile(self, prowjs,pwdb,pParameter):

        self.Parameter=pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10,lv_job_worker_last_run_start_dt,lv_job_last_success_run_dt,lv_job_group_cd,lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs

            inputFileNameFilter = self.Parameter.getParameter('ETL_OUTPUT') + "\\" + lv_job_loc_cd + "__" + lv_job_program_arg1 + "__" + "*.xml"
            successFolderName=self.Parameter.getParameter('ETL_OUTPUT')+"\\"+"success"+"\\"
            for inputXMLFileName in glob.glob(inputFileNameFilter):
                try:
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

                    file_name_list = []
                    file_name_list.append(inputXMLBaseFileName)
                    file_name_list.append(inputChecksumBaseFileName)

                    file_content_b64_list = []
                    with open(inputXMLFileName, 'rb') as f:
                        file_content_b64 = base64.b64encode(f.read())
                        file_content_b64_list.append(file_content_b64)

                    with open(inputChecksumFileName, 'rb') as f:
                        file_content_b64 = base64.b64encode(f.read())
                        file_content_b64_list.append(file_content_b64)


                    data = {"file_name": file_name_list, "file_content": file_content_b64_list}
                    access_url = pwdb.dsrv["access_url"]
                    access_token=pwdb.dsrv["access_token"]
                    print(access_url)
                    print(access_token)
                    headers = {}
                    headers["access-token"] = access_token

                    response = requests.post(access_url, data=data, headers=headers)
                    self.log.Write(response.text)
                    if response.status_code != 200:
                        self.log.Write(response.text)
                        print(response.text)
                        continue

                    # Move Success files to input\success folder
                    os.rename(inputXMLFileName,successFolderName+inputXMLBaseFileName)
                    os.rename(inputChecksumFileName, successFolderName + inputChecksumBaseFileName)

                except Exception as e:
                    self.log.Write(traceback.print_exc())
                    self.log.Write("Error occured while sending : "+ inputXMLFileName)
                    self.log.Write(inputXMLFileName + " : Skiping...")
                    print("Error occured while sending : "+ inputXMLFileName)
                    print(inputXMLFileName + " : Skiping...")

                    None

            # END: fname

            status=0
            errorCd=""
            errorMsg=""


            return status,errorCd,errorMsg

        except BaseException as e:
            status=1
            errorCd=str(e)
            errorMsg=str(e)

            print(errorMsg)
            self.log.Write(traceback.print_exc())

        finally:
            return status, errorCd, errorMsg
    #END: SendFile
