export ETL_HOME=/data/ETL/APP
echo BEGIN: `date` >> /data/ETL/APP/log/A_KIMS_PGSQL-DA2DL_DA.log
python3 /data/ETL/APP/source/AgentCLI.py A_KIMS_PGSQL-DA2DL_DA >> /data/ETL/APP/log/A_KIMS_PGSQL-DA2DL_DA.log
status=$?
echo $status >> /data/ETL/APP/log/A_KIMS_PGSQL-DA2DL_DA.log
echo END:`date` >> /data/ETL/APP/log/A_KIMS_PGSQL-DA2DL_DA.log
