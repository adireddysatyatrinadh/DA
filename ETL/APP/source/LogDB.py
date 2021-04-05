#from ETL.bin import Config
import traceback
from Parameter import Parameter

if ((Parameter.getETLParameter('source_db.type').lower()=="oracle") or (Parameter.getETLParameter('destination_db.type').lower() == "oracle") or (Parameter.getETLParameter('etl_db.type').lower() == "oracle")):
        import cx_Oracle

if ((Parameter.getETLParameter('source_db.type').lower()=="postgresql") or (Parameter.getETLParameter('destination_db.type').lower() == "postgresql") or (Parameter.getETLParameter('etl_db.type').lower() == "postgresql")):
    import psycopg2

if Parameter.getETLParameter('source_db.type').lower() == "elasticsearch" or Parameter.getETLParameter('destination_db.type').lower() == "elasticsearch" or Parameter.getETLParameter('etl_db.type').lower() == "elasticsearch":
    from elasticsearch import Elasticsearch

class LogDB:

    def __init__(self,pLog):
        self.log=pLog
        self.Parameter=Parameter(None)

    def GetLogDBConnection(self):
            return self.lcon

    def OpenLogDBConnection(self):
        # Log Databsase Connection
        self.log.Write("In Log DB Connection")
        if self.Parameter.getParameter('etl_db.type').lower()=="oracle":
            self.lcon = cx_Oracle.connect(
            self.Parameter.getParameter('etl_db.username'),
            self.Parameter.getParameter('etl_db.password'),
            self.Parameter.getParameter('etl_db.host') + ":" + self.Parameter.getParameter('etl_db.port') + "/" + self.Parameter.getParameter('etl_db.name'),
            encoding=self.Parameter.getParameter('etl_db.encoding'))
            self.log.Write("Log DB Connection: Opened")
            return self.lcon

        if self.Parameter.getParameter('etl_db.type').lower() == "postgresql":
            constring = ""
            constring += "dbname=" + self.Parameter.getParameter('etl_db.name') + " "
            constring += "user=" + self.Parameter.getParameter('etl_db.username') + " "
            constring += "host=" + self.Parameter.getParameter('etl_db.host') + " "
            constring += "port=" + self.Parameter.getParameter('etl_db.port') + " "
            constring += "password=" + self.Parameter.getParameter('etl_db.password') + " "

            self.lcon = psycopg2.connect(constring)
            lcur = self.lcon.cursor()

            sql = ""
            sql += "CREATE  TEMPORARY TABLE IF NOT EXISTS TEMP_ETL_JOB"
            sql += "("
            sql += " job_cd varchar(256) NOT NULL"
            sql += ",job_group_cd varchar(32)"
            sql += ",job_worker_cd varchar(256)"
            sql += ",grp_cd varchar(16) NOT NULL"
            sql += ",CONSTRAINT temp_etl_job_pk PRIMARY KEY (job_cd, grp_cd)"
            sql += ") ON COMMIT PRESERVE ROWS"

            self.log.Write(sql)
            lcur.execute(sql)

            sql = ""
            sql += "CREATE TEMPORARY TABLE IF NOT EXISTS temp_etl_job_worker_jobs"
            sql += "("
            sql += " job_cd varchar(256) NOT NULL"
            sql += ",job_worker_cd varchar(256)"
            sql += ",grp_cd varchar(16) NOT NULL"
            sql += ",CONSTRAINT temp_etl_job_worker_jobs_pk PRIMARY KEY (job_worker_cd,job_cd, grp_cd)"
            sql += ") ON COMMIT PRESERVE ROWS"

            self.log.Write(sql)
            lcur.execute(sql)

            sql = ""
            sql += "CREATE  TEMPORARY TABLE IF NOT EXISTS temp_etl_worker_load"
            sql += "("
            sql += " job_worker_cd varchar(256)"
            sql += ",job_cnt integer"
            sql += ",CONSTRAINT temp_etl_worker_load_pk PRIMARY KEY (job_worker_cd)"
            sql += ")"

            self.log.Write(sql)
            lcur.execute(sql)
            lcur.close()

            self.log.Write("Log DB Connection: Opened")
            return self.lcon


    def CloseLogDBConnection(self):
        # Log Database Connection
        if  self.lcon:
            self.lcon.close()


    def getJobAgentCd(self,p_job_worker_cd):

        self.log.Write("BEGIN: getJobAgentCd")
        lcur = self.lcon.cursor()
        #   Fetch Next Job Run No
        if self.Parameter.getParameter('etl_db.type').lower() == "oracle":
            sql = ""
            sql += " SELECT job_agent_cd"
            sql += " FROM etl_job_worker jw"
            sql += " WHERE jw.job_worker_cd = :job_worker_cd"
            sql += " AND jw.record_status = 'A'"
        elif self.Parameter.getParameter('etl_db.type').lower() == "postgresql":
            sql = ""
            sql += " SELECT job_agent_cd"
            sql += " FROM etl_job_worker jw"
            sql += " WHERE jw.job_worker_cd = %(job_worker_cd)s"
            sql += " AND jw.record_status = 'A'"

        ldic = {}
        ldic['job_worker_cd'] = p_job_worker_cd
        # print(1)
        self.log.Write(sql, ldic)
        lcur.execute(sql, ldic)
        self.log.Write("rowcount=", lcur.rowcount)

        lv_job_agent_cd, = lcur.fetchone()
        self.log.Write(lv_job_agent_cd)
        # print(2)
        lcur.close()

        self.log.Write("END: getJobAgentCd")
        return lv_job_agent_cd

    def getJobAgentGrpCd(self,p_job_agent_cd):

        self.log.Write("BEGIN: getJobAgentGrpCd")
        lcur = self.lcon.cursor()
        #   Fetch Next Job Run No
        if self.Parameter.getParameter('etl_db.type').lower() == "oracle":
            sql = ""
            sql += " SELECT grp_cd"
            sql += " FROM etl_job_agent ja"
            sql += " WHERE ja.job_agent_cd = :job_agent_cd"
            sql += " AND ja.record_status = 'A'"
        elif self.Parameter.getParameter('etl_db.type').lower() == "postgresql":
            sql = ""
            sql += " SELECT grp_cd"
            sql += " FROM etl_job_agent ja"
            sql += " WHERE ja.job_agent_cd = %(job_agent_cd)s"
            sql += " AND ja.record_status = 'A'"

        ldic = {}
        ldic['job_agent_cd'] = p_job_agent_cd
        # print(1)
        self.log.Write(sql, ldic)
        lcur.execute(sql, ldic)
        self.log.Write("rowcount=", lcur.rowcount)

        lv_job_agent_grp_cd, = lcur.fetchone()
        self.log.Write(lv_job_agent_grp_cd)
        # print(2)
        lcur.close()

        self.log.Write("END: getJobAgentGrpCd")
        return lv_job_agent_grp_cd
