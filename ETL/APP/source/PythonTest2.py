from elasticsearch import Elasticsearch


es = Elasticsearch(['http://localhost:9200'])

dic={}
dic["emp_no"]=10
dic["emp_name"]="ram"
es.index(index="abc",id=1,body=dic)
print()
