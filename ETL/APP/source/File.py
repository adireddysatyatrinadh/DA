import os
import traceback

class File:

    def __init__(self,pLog):
        self.log=pLog
        if(self.log is not None):
            self.log.Write("File: Constructor")
    def ReadXMLFile(self,filename):
        try:
            ftext=''
            ifile=open(filename,"r")
            for line in ifile.readlines():
                ftext=ftext+line
            ifile.close()
            #if (self.log is not None):
            #    self.log.Write(ftext)
            return ftext
        except BaseException as e:
            if (self.log is not None):
                self.log.Write(traceback.print_exc())


    def ReadConfigFile(self,filename):
        try:
            ldic={}
            ifile=open(filename,"r")
            for line in ifile.readlines():
                if (self.log is not None):
                    self.log.Write(line)
                if ( len(line) > 0):
                    pos=line.find('=')
                    if ( pos > -1 ):
                        key=line[:pos].strip().upper()
                        value=line[pos+1:].strip()

                        if (key=='ETL_METADATA'):
                            value = value.replace("%ETL_HOME%", os.environ['ETL_HOME'])

                        ldic[key]=value

            ifile.close()
            if (self.log is not None):
                self.log.Write(ldic)
            return ldic
        except BaseException as e:
            if (self.log is not None):
                self.log.Write(e.with_traceback())

    def ReadConfigFiles(self,filename1,filename2):
        try:

            Config={}
            f1Config = self.ReadConfigFile(filename1)
            f2Config = self.ReadConfigFile(filename2)
            Config.update(f1Config)
            Config.update(f2Config)

            return Config

        except BaseException as e:
            if (self.log is not None):
                self.log.Write(traceback.print_exc())

