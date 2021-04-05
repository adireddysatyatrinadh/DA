import os
from File import File
import traceback

class Parameter:

    def __init__(self,pAgentName=None,pLog=None):
        self.log=pLog
        if self.log:
            self.log.Write("BEGIN: Parameter Constructor")

        try:
            etlHomeFolderName = os.environ['ETL_HOME']
            etlOS = os.environ['ETL_OS']

            if (pAgentName is not None):
                if etlOS.lower() == "windows":
                    agentConfigFileName = etlHomeFolderName + "\config\\" + pAgentName + ".config"
                    etlConfigFileName = etlHomeFolderName + "\config\ETL.config"
                elif etlOS.lower() == "linux":
                    agentConfigFileName = etlHomeFolderName + "/config/" + pAgentName + ".config"
                    etlConfigFileName = etlHomeFolderName + "/config/ETL.config"
                self.Parameter = File(self.log).ReadConfigFiles(etlConfigFileName,agentConfigFileName)
            else:
                if etlOS.lower() == "windows":
                    etlConfigFileName = etlHomeFolderName + "\config\ETL.config"
                elif etlOS.lower() == "linux":
                    etlConfigFileName = etlHomeFolderName + "/config/ETL.config"
                self.Parameter = File(self.log).ReadConfigFile(etlConfigFileName)

            self.Parameter['ETL_HOME'] = etlHomeFolderName
            self.Parameter['ETL_OS'] = etlOS.lower()


        except BaseException as e:
            self.log.Write(traceback.print_exc())
        finally:
            if self.log:
                self.log.Write("END: Parameter Constructor")

    def getParameter(self,pParameterName):
        try:
            return self.Parameter[pParameterName.upper()]

        except BaseException as e:
            self.log.Write(e.__str__())
            self.log.Write(traceback.print_exc())

    @staticmethod
    def getETLParameter(pParameterName):
        try:
            etlHomeFolderName = os.environ['ETL_HOME']
            etlOS = os.environ['ETL_OS']
            print(etlHomeFolderName)
            print(etlOS)
            if etlOS.lower() == "windows":
                etlConfigFileName = etlHomeFolderName + "\config\ETL.config"
            elif etlOS.lower() == "linux":
                etlConfigFileName = etlHomeFolderName + "/config/ETL.config"
            #print("os=",etlOS)
            #print("configfile=",etlConfigFileName)
            lv_parameter_value=""
            ifile = open(etlConfigFileName, "r")
            for line in ifile.readlines():
                if (len(line) > 0):
                    pos = line.find('=')
                    if (pos > -1):
                        key = (line[:pos].strip()).upper()
                        value = line[pos + 1:].strip()

                        if (key == 'ETL_METADATA'):
                            value = value.replace("%ETL_HOME%", os.environ['ETL_HOME'])

                        if(key==pParameterName.upper()):
                            lv_parameter_value=value
                            break

            ifile.close()
            print(pParameterName,lv_parameter_value)
            return lv_parameter_value

        except BaseException as e:
            print(e.__str__())
            #if self.log:
            #   self.log.Write(traceback.print_exc())
