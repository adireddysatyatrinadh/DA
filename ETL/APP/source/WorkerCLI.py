
from File import File
from Date import Date
from Worker import Worker
from LogFile import LogFile
import sys
import datetime
import os
import traceback


# Python ETL\APP\bin\WorkerCLI.py W_KIMS.KSMC_ORA_DB2DB_HIS_M
# Python ETL\APP\bin\WorkerCLI.py W_KIMS_PGSQL2ES
# Python ETL\APP\bin\WorkerCLI.py W_KIMS.KIMSKU_DCDB2CDB_SAP
try:
    if len(sys.argv) !=2:
        print("Invalid Number of Arugments")
        print("Usage:")
        print("Python Worker.py <job_worker_cd>)")
        print("eg.,:")
        print("Python ETL\\bin\WorkerCLI.py W_KIMS.KSMC_ORA_DB2DB_HIS_M")
        sys.exit("Exiting...")


    cp_job_worker_cd = sys.argv[1]
    #cp_job_worker_cd = "W_KIMS.KSMC_ORA_DB2DB_HIS_M"
    log = LogFile(cp_job_worker_cd)
    log.Write("BEGIN:WorkerCLI Code")
    w=Worker(cp_job_worker_cd,log)
    w.Execute()


except SystemExit as se:
       None
except BaseException as e:
        log.Write(e.__str__())
        self.log.Write(traceback.print_exc())

finally:
    None
    log.Write("END:WorkerCLI Code")
    log.Close()
