from File import File
from Date import Date
from Parameter import Parameter
from Agent import Agent
from LogFile import LogFile
import sys
import datetime
import os
import traceback

# from ETL.APP.bin.DB import DB

# Python ETL\APP\bin\AgentCLI.py A_KIMS.KSMC_ORA_DB2DB
# Python ETL\APP\bin\AgentCLI.py A_KIMS_PGSQL2ES
# Python ETL\APP\bin\AgentCLI.py A_KIMS.KIMSKU_DCDB2CDB_SAP
try:
    if len(sys.argv) !=2:
        print("Invalid Number of Arugments")
        print("Usage:")
        print("Python Agent.py <job_Agent_cd>)")
        print("eg.,:")
        print("Python ETL\\bin\AgentCLI.py A_KIMS.KSMC_ORA_DB2DB")
        sys.exit("Exiting...")

    cp_job_agent_cd = sys.argv[1]
    #pjob_agent_cd = "A_KIMS.KSMC_ORA_DB2DB"
    log = LogFile(cp_job_agent_cd)
    log.Write("BEGIN:AgentCLI Code")

    a = Agent(cp_job_agent_cd, log)
    a.Execute()

except SystemExit as se:
       log.Write("Exception:AgentCLI")
except BaseException as e:
       log.Write(e)
       self.log.Write(traceback.print_exc())
finally:
    log.Write("END:AgentCLI Code")
    log.Close()
    None
