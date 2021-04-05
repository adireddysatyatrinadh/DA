import psycopg2 # for database connection and dependent on Flask

o_conn = psycopg2.connect(host="localhost",
    database="datalake_p2",
    user="postgres",
    password="p@ssw0rd")
o_cursor = o_conn.cursor()
# ^ usually no need to repeat this ^

list=list()
list.append(10)
list.append(20)


o_cursor.execute("select * from  public.insert_data(%s,%s)", tuple(list))
s,ec,em = o_cursor.fetchone()
o_cursor.close()
# usually no need to repeat code below here
o_conn.close()

print(s)
print(ec)
print(em)

