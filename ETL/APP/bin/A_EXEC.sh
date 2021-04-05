echo BEGIN: `date` >> $ETL_HOME/log/$1.log
python3 $ETL_HOME/source/AgentCLI.py $1 >> $ETL_HOME/log/$1.log
status=$?
echo $status $ETL_HOME/log/$1.log
echo END:`date` >> $ETL_HOME/log/$1.log
