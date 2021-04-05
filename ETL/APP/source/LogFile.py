import os
import datetime
from Parameter import Parameter
#from multipledespatch import dispatch
import traceback

class LogFile:

    def __init__(self,name):
        p = Parameter(None)
        dt = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        if p.getParameter("OS").lower() == "windows":
            logFileName = p.getParameter('ETL_LOG') + "\\" + dt+"_"+name+".log"
        elif p.getParameter("OS").lower() == "linux":
            logFileName = p.getParameter('ETL_LOG') + "/" + dt + "_" + name + ".log"
        self.lfile = open(logFileName, "w")
        print("LogFileName=",logFileName)

    def Write(self,ltext1=None,ltext2=None,ltext3=None,ltext4=None):
        try:
            cdt=datetime.datetime.now()
            cdtstr=cdt.strftime('%Y:%m:%d %H:%M:%S.%f')

            if (ltext1 is None):
                None
            elif(type(ltext1) is datetime.datetime):
                ltext1=ltext1.strftime('%Y%m%d%H%M%S%f')
            elif (type(ltext1) is datetime.date):
                ltext1 = ltext1.strftime('%Y%m%d')
            elif (type(ltext1) is datetime.time):
                ltext1 = ltext1.strftime('%H%M%S%f')
            elif (type(ltext1) is dict):
                ltext1 = str(ltext1)
            else:
                ltext1 = str(ltext1)

            if (ltext2 is None):
                None
            elif(type(ltext2) is datetime.datetime):
                ltext2=ltext2.strftime('%Y%m%d%H%M%S%f')
            elif (type(ltext2) is datetime.date):
                ltext2 = ltext2.strftime('%Y%m%d')
            elif (type(ltext2) is datetime.time):
                ltext2 = ltext2.strftime('%H%M%S%f')
            elif (type(ltext2) is dict):
                ltext2 = str(ltext2)
            else:
                ltext2 = str(ltext2)

            if (ltext3 is None):
                None
            elif(type(ltext3) is datetime.datetime):
                ltext3=ltext3.strftime('%Y%m%d%H%M%S%f')
            elif (type(ltext3) is datetime.date):
                ltext3 = ltext3.strftime('%Y%m%d')
            elif (type(ltext3) is datetime.time):
                ltext3 = ltext3.strftime('%H%M%S%f')
            elif (type(ltext3) is dict):
                ltext3 = str(ltext3)
            else:
                ltext3 = str(ltext3)

            if (ltext4 is None):
                None
            elif(type(ltext4) is datetime.datetime):
                ltext4=ltext4.strftime('%Y%m%d%H%M%S%f')
            elif (type(ltext4) is datetime.date):
                ltext4 = ltext4.strftime('%Y%m%d')
            elif (type(ltext4) is datetime.time):
                ltext4 = ltext4.strftime('%H%M%S%f')
            elif (type(ltext4) is dict):
                ltext4 = str(ltext4)
            else:
                ltext4 = str(ltext4)

            line=""
            if (ltext1 != None and ltext2 != None and ltext3 != None and ltext4 != None):
                line = cdtstr + " " + ltext1 + ltext2 + ltext3 + ltext4 + "\n"
            elif (ltext1 != None and ltext2 != None and ltext3!=None):
                line=cdtstr+" "+ltext1+ltext2+ltext3+"\n"
            elif (ltext1!=None and ltext2!=None):
                line=cdtstr+" "+ltext1+ltext2+"\n"
            elif (ltext1!=None):
                line=cdtstr + " " + ltext1 + "\n"

            self.lfile.write(line)

        except BaseException as e:
            print(e)
            print(traceback.print_exc())

    def Close(self):
        try:
            self.lfile.close()
        except BaseException as e:
            print(e)
            print(traceback.print_exc())

