python3 ../source/AgentCLI.py A_KIMS.KIMSKU_FILE2DB_DA
rt ETL_HOME=/data/ETL/APP
echo BEGIN: `date` >> /data/ETL/APP/log/A_KIMS.KIMSKU_FILE2DB_DA.log
python3 /data/ETL/APP/source/AgentCLI.py A_KIMS.KIMSKU_FILE2DB_DA >> /data/ETL/APP/log/A_KIMS.KIMSKU_FILE2DB_DA.log
status=$?
echo $status >> /data/ETL/APP/log/A_KIMS.KIMSKU_FILE2DB_DA.log
echo END:`date` >> /data/ETL/APP/log/A_KIMS.KIMSKU_FILE2DB_DA.log