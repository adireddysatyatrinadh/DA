#from ETL.bin import Config
#import xml.etree.ElementTree as ET
#from ETL.bin import Config
from File import File
from DB import DB
from Parameter import Parameter
from Table2TableSync import Table2TableSync
import sys

# Python ETL\bin\DB2DBSyncCLI.py countrym.xml 1900-01-01 2020-12-31 KSMC HIMS_DA_INT_ATP HIMS_DA_INT_ALL
try:

    print("BEGIN:DB2DBSyncCLI")

    db=None
    p=None

    print("before argument Validation")
    if len(sys.argv) !=7:
        print("Invalid Number of Arugments")
        print("Usage:")
        print("DB2DBSync.py <xmlFileName> <LastUpdatedFromDt> <LastUpdatedToDt> <GroupCode> <SourceSchemaName> <DestinationSchemaName>)")
        print("eg.,:")
        print("DB2DBSync.py countrym.xml 2000-01-01 2020-12-31 MCHM HIMS_DA_INT_ATP HIMS_DA_INT_ALL")
        sys.exit("Exiting...")

    print("after argument Validation")

    # Load Configuration Parameters
    p = Parameter("A_KIMS.ATP_ORA_DB2DB")

    #etlHomeFolderName=p.getParameter('ETL_HOME')
    #etlDataFolderName=p.getParameter('ETL_DATA')
    xmlFileName=p.getParameter('ETL_DATA')+"\\"+sys.argv[1]
    print(xmlFileName)
    fromdt=sys.argv[2]
    todt=sys.argv[3]
    grp_cd=sys.argv[4]
    source_schema_name=sys.argv[5]
    destintation_schema_name=sys.argv[6]

    print(p,xmlFileName,fromdt,todt,grp_cd)

    db= DB(p)
    db.OpenSourceDBConnection()
    db.OpenDestinationDBConnection()

    db2db=DB2DBSync()
    status, errorCd, errorMsg=db2db.SyncTable(xmlFileName,fromdt,todt,source_schema_name,destintation_schema_name,grp_cd,grp_cd,grp_cd,db)

    print(status,errorCd,errorMsg)

    print("END:DB2DBSyncCLI")
except SystemExit as se:
       None
except BaseException as e:
       print(e.with_traceback())
finally:
    #print("In Finally")
    if db:
        db.CloseSourceDBConnection()
        db.CloseDestinationDBConnection()
