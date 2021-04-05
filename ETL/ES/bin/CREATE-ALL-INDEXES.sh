ETL_ES_LOG=$ETL_ES_HOME/log
LOGFILENAME=$ETL_ES_LOG/CREATE-ALL-INDEXES.log

> $LOGFILENAME

HOST=$1
PORT=$2
ETL_ES_INDEXES=$ETL_ES_HOME/source/indexes

echo $HOST >> $LOGFILENAME
echo $PORT >> $LOGFILENAME

for FILENAME in $ETL_ES_INDEXES/*.map; do
INDEXNAME=$(echo $FILENAME | rev | cut -d'/' -f 1 | cut -d'.' -f 2 | rev)
echo $FILENAME,$INDEXNAME >> $LOGFILENAME
URL=http://$HOST:$PORT/$INDEXNAME?pretty
curl --silent -XPUT $URL -H 'Content-Type: application/json' -d @$FILENAME -n >> $LOGFILENAME
done
