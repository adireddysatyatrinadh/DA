export ETL_HOME=/data/ETL/APP
echo BEGIN: `date` >> /data/ETL/APP/log/A_KIMS.BHC_FILE2DB_DA.log
python3 /data/ETL/APP/source/AgentCLI.py A_KIMS.BHC_FILE2DB_DA >> /data/ETL/APP/log/A_KIMS.BHC_FILE2DB_DA.log
status=$?
echo $status >> /data/ETL/APP/log/A_KIMS.BHC_FILE2DB_DA.log
echo END:`date` >> /data/ETL/APP/log/A_KIMS.BHC_FILE2DB_DA.log


