from File import File
from LogDB import LogDB
from Date import Date
from Parameter import Parameter
import sys
import datetime
import os
import traceback

class Agent:

    def __init__(self, pJobAgentCd,pLog):
        self.log=pLog
        self.log.Write("BEGIN:Agent Constructor")
        self.cv_job_agent_cd=pJobAgentCd

        self.p = Parameter(self.cv_job_agent_cd,self.log)
        self.logdb = LogDB(self.log)
        self.logdb.OpenLogDBConnection()

        self.log.Write("END:Agent Constructor")

    def Execute(self):
        try:
            self.log.Write("Begin: Agent Execute")

            lv_job_agent_start_datetime=datetime.datetime.now()
            self.log.Write("lv_job_agent_start_datetime=",lv_job_agent_start_datetime)
            
            dt = Date(self.log)
            lcur = self.logdb.lcon.cursor()
            lcur2 = self.logdb.lcon.cursor()


            if self.p.getParameter('etl_db.type').lower() == "oracle":
                sql=""
                sql+=" UPDATE etl_job_agent "
                sql+=" SET"
                sql+=" last_run_status = 'R'"
                sql+=" ,last_run_start_dt = :job_agent_start_datetime"
                sql+=" ,last_run_no = COALESCE(last_run_no,0)+1"
                sql+=" WHERE job_agent_cd = :job_agent_cd"
                sql+=" AND record_status = 'A'"
                sql+=" AND last_run_status NOT IN('R')"
            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                sql=""
                sql+=" UPDATE etl_job_agent "
                sql+=" SET"
                sql+=" last_run_status = 'R'"
                sql+=" ,last_run_start_dt = %(job_agent_start_datetime)s"
                sql+=" ,last_run_no = COALESCE(last_run_no,0)+1"
                sql+=" WHERE job_agent_cd = %(job_agent_cd)s"
                sql+=" AND record_status = 'A'"
                sql+=" AND last_run_status NOT IN('R')"


            ldic={}
            ldic['job_agent_cd'] = self.cv_job_agent_cd
            ldic['job_agent_start_datetime'] = lv_job_agent_start_datetime
            self.log.Write(sql,ldic)
            lcur.execute(sql,ldic)
            self.logdb.lcon.commit()
            self.log.Write("rowcount=",lcur.rowcount)

            if lcur.rowcount > 0: # Agent is not in Running Mode so execute Agent code
                #   Fetch Agent Schedule Code
                if self.p.getParameter('etl_db.type').lower() == "oracle":
                    sql = ""
                    sql += " SELECT last_run_no,next_run_dt,sch_cd,grp_cd"
                    sql += " FROM etl_job_agent"
                    sql += " WHERE job_agent_cd = :job_agent_cd"
                    sql += " AND record_status = 'A'"
                if self.p.getParameter('etl_db.type').lower() == "postgresql":
                    sql = ""
                    sql += " SELECT last_run_no,next_run_dt,sch_cd,grp_cd"
                    sql += " FROM etl_job_agent"
                    sql += " WHERE job_agent_cd = %(job_agent_cd)s"
                    sql += " AND record_status = 'A'"

                ldic = {}
                ldic['job_agent_cd'] = self.cv_job_agent_cd
                self.log.Write(sql,ldic)
                lcur.execute(sql, ldic)
                self.log.Write("rowcount=",lcur.rowcount)

                lv_job_agent_run_no,lv_job_agent_next_run_datetime,lv_job_agent_sch_cd,lv_job_agent_grp_cd = lcur.fetchone()
                self.log.Write("job_agent_run_no=",lv_job_agent_run_no)
                self.log.Write(lv_job_agent_next_run_datetime,lv_job_agent_sch_cd,lv_job_agent_grp_cd)

                # Delete data from ETL_JOB Temp tables
                sql=""
                sql+="DELETE from TEMP_ETL_JOB"
                self.log.Write(sql)
                lcur.execute(sql)

                self.log.Write("rowcount=",lcur.rowcount)

                #insert into temp table, whatever the rundt less than are equal to sysdate and also run status sucess or failure
                if self.p.getParameter('etl_db.type').lower() == "oracle":
                    sql=""
                    sql+=" INSERT INTO TEMP_ETL_JOB(job_cd, job_group_cd,grp_cd, job_worker_cd)"
                    sql+=" SELECT DISTINCT j.job_cd, j.job_group_cd,j.grp_cd, NULL "
                    sql+=" FROM ETL_JOB_WORKER jw"
                    sql+=" INNER JOIN etl_job_worker_jobs jwj"
                    sql+=" ON(jw.job_worker_cd = jwj.job_worker_cd AND jwj.record_status = 'A' AND jw.grp_cd=jwj.grp_cd)"
                    sql+=" INNER JOIN ETL_JOB j"
                    sql+=" ON ((j.job_cd LIKE COALESCE(jwj.job_cd,'') OR j.job_group_cd LIKE COALESCE(jwj.job_group_cd,'')) AND jw.grp_cd=j.grp_cd)"
                    sql+=" WHERE jw.record_status = 'A'"
                    sql+=" AND jw.job_agent_cd = :job_agent_cd"
                    sql+=" AND jw.grp_cd=:grp_cd"
                    sql+=" AND j.next_run_dt <= :job_agent_start_datetime"
                    sql+=" AND j.last_run_status NOT IN('C', 'Q', 'R')"
                    sql+=" AND j.grp_cd=:grp_cd"
                elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                    sql=""
                    sql+=" INSERT INTO TEMP_ETL_JOB(job_cd, job_group_cd,grp_cd, job_worker_cd)"
                    sql+=" SELECT DISTINCT j.job_cd, j.job_group_cd,j.grp_cd, NULL "
                    sql+=" FROM ETL_JOB_WORKER jw"
                    sql+=" INNER JOIN etl_job_worker_jobs jwj"
                    sql+=" ON(jw.job_worker_cd = jwj.job_worker_cd AND jwj.record_status = 'A' AND jw.grp_cd=jwj.grp_cd)"
                    sql+=" INNER JOIN ETL_JOB j"
                    sql+=" ON ((j.job_cd LIKE COALESCE(jwj.job_cd,'') OR j.job_group_cd LIKE COALESCE(jwj.job_group_cd,'')) AND jw.grp_cd=j.grp_cd)"
                    sql+=" WHERE jw.record_status = 'A'"
                    sql+=" AND jw.job_agent_cd = %(job_agent_cd)s"
                    sql+=" AND jw.grp_cd= %(grp_cd)s"
                    sql+=" AND j.next_run_dt <= %(job_agent_start_datetime)s"
                    sql+=" AND j.last_run_status NOT IN('C', 'Q', 'R')"
                    sql+=" AND j.grp_cd=%(grp_cd)s"

                ldic={}
                ldic['job_agent_start_datetime']=lv_job_agent_start_datetime
                ldic['grp_cd']=lv_job_agent_grp_cd
                ldic['job_agent_cd'] = self.cv_job_agent_cd
                self.log.Write(sql,ldic)
                lcur.execute(sql,ldic)
                self.log.Write("rowcount=",lcur.rowcount)

                # Jobs are waiting to execute
                if (lcur.rowcount > 0):
                    sql = ""
                    sql += "DELETE from TEMP_ETL_JOB_WORKER_JOBS"
                    self.log.Write(sql)
                    lcur.execute(sql)
                    self.log.Write("rowcount=", lcur.rowcount)

                    # insert into temp table, whatever the worker jobs against the selected agent
                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql=""
                        sql+=" INSERT INTO TEMP_ETL_JOB_WORKER_JOBS(job_worker_cd, job_cd,grp_cd)"
                        sql+=" SELECT jw.job_worker_cd, j.job_cd, j.grp_cd"
                        sql+=" FROM ETL_JOB_WORKER jw"
                        sql+=" INNER JOIN etl_job_worker_jobs jwj"
                        sql+=" ON(jw.job_worker_cd = jwj.job_worker_cd AND jwj.record_status = 'A' AND jw.grp_cd=jwj.grp_cd)"
                        sql+=" INNER JOIN TEMP_ETL_JOB j"
                        sql+=" ON ((j.job_cd LIKE COALESCE(jwj.job_cd,'') OR j.job_group_cd LIKE COALESCE(jwj.job_group_cd,'')) AND jw.grp_cd=j.grp_cd)"
                        sql+=" WHERE jw.record_status = 'A'"
                        sql+=" AND jw.job_agent_cd = :job_agent_cd"
                        sql+=" AND jw.grp_cd=:grp_cd"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql=""
                        sql+=" INSERT INTO TEMP_ETL_JOB_WORKER_JOBS(job_worker_cd, job_cd,grp_cd)"
                        sql+=" SELECT jw.job_worker_cd, j.job_cd, j.grp_cd"
                        sql+=" FROM ETL_JOB_WORKER jw"
                        sql+=" INNER JOIN etl_job_worker_jobs jwj"
                        sql+=" ON(jw.job_worker_cd = jwj.job_worker_cd AND jwj.record_status = 'A' AND jw.grp_cd=jwj.grp_cd)"
                        sql+=" INNER JOIN TEMP_ETL_JOB j"
                        sql+=" ON ((j.job_cd LIKE COALESCE(jwj.job_cd,'') OR j.job_group_cd LIKE COALESCE(jwj.job_group_cd,'')) AND jw.grp_cd=j.grp_cd)"
                        sql+=" WHERE jw.record_status = 'A'"
                        sql+=" AND jw.job_agent_cd = %(job_agent_cd)s"
                        sql+=" AND jw.grp_cd=%(grp_cd)s"

                    ldic = {}
                    ldic['job_agent_cd'] = self.cv_job_agent_cd
                    ldic['grp_cd'] = lv_job_agent_grp_cd
                    self.log.Write(sql, ldic)
                    lcur.execute(sql, ldic)

                    self.log.Write("rowcount=",lcur.rowcount)

                    # Delete data from ETL_JOB Temp tables
                    sql=""
                    sql+="DELETE from TEMP_ETL_WORKER_LOAD"
                    self.log.Write(sql)
                    lcur.execute(sql)
                    self.log.Write("rowcount=", lcur.rowcount)

                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql=""
                        sql+=" INSERT INTO TEMP_ETL_WORKER_LOAD(job_worker_cd, job_cnt)"
                        sql+=" SELECT jw.job_worker_cd, COUNT(*) as job_cnt"
                        sql+=" FROM ETL_JOB_WORKER jw"
                        sql+=" LEFT JOIN ETL_JOB j ON(jw.job_worker_cd = j.job_worker_cd AND j.last_run_status IN('C', 'Q', 'R') AND j.grp_cd=:grp_cd)"
                        sql+=" WHERE jw.record_status = 'A'"
                        sql+=" AND jw.grp_cd = :grp_cd"
                        sql+=" AND jw.job_agent_cd = :job_agent_cd"
                        sql+=" GROUP BY jw.job_worker_cd"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql=""
                        sql+=" INSERT INTO TEMP_ETL_WORKER_LOAD(job_worker_cd, job_cnt)"
                        sql+=" SELECT jw.job_worker_cd, COUNT(*) as job_cnt"
                        sql+=" FROM ETL_JOB_WORKER jw"
                        sql+=" LEFT JOIN ETL_JOB j ON(jw.job_worker_cd = j.job_worker_cd AND j.last_run_status IN('C', 'Q', 'R') AND j.grp_cd=%(grp_cd)s)"
                        sql+=" WHERE jw.record_status = 'A'"
                        sql+=" AND jw.grp_cd = %(grp_cd)s"
                        sql+=" AND jw.job_agent_cd = %(job_agent_cd)s"
                        sql+=" GROUP BY jw.job_worker_cd"

                    ldic = {}
                    ldic['job_agent_cd'] = self.cv_job_agent_cd
                    ldic['grp_cd'] = lv_job_agent_grp_cd

                    self.log.Write(sql, ldic)
                    lcur.execute(sql,ldic)

                    self.log.Write("rowcount=",lcur.rowcount)

                    sql=""
                    sql+=" SELECT job_cd, grp_cd"
                    sql+=" FROM TEMP_ETL_JOB"
                    self.log.Write(sql)
                    lcur2.execute(sql)
                    self.log.Write("rowcount=",lcur2.rowcount)


                    while True:

                        lrow=lcur2.fetchone()
                        if lrow is None:
                            break
                        lv_temp_job_cd,lv_temp_grp_cd=lrow

                        #getting the lowest jobs cnt workers

                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql=""
                            sql+=" SELECT jwj.job_worker_cd"
                            sql+=" FROM TEMP_ETL_JOB_WORKER_JOBS jwj"
                            sql+=" INNER JOIN TEMP_ETL_WORKER_LOAD wl ON(jwj.job_worker_cd = wl.job_worker_cd)"
                            sql+=" WHERE jwj.job_cd = :job_cd"
                            sql+=" AND jwj.grp_cd = :grp_cd"
                            sql+=" ORDER BY wl.job_cnt ASC"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql=""
                            sql+=" SELECT jwj.job_worker_cd"
                            sql+=" FROM TEMP_ETL_JOB_WORKER_JOBS jwj"
                            sql+=" INNER JOIN TEMP_ETL_WORKER_LOAD wl ON(jwj.job_worker_cd = wl.job_worker_cd)"
                            sql+=" WHERE jwj.job_cd = %(job_cd)s"
                            sql+=" AND jwj.grp_cd = %(grp_cd)s"
                            sql+=" ORDER BY wl.job_cnt ASC"

                        ldic = {}
                        ldic['job_cd'] = lv_temp_job_cd
                        ldic['grp_cd'] = lv_temp_grp_cd

                        self.log.Write(sql, ldic)
                        lcur.execute(sql,ldic)

                        self.log.Write("rowcount=", lcur.rowcount)
                        lv_temp_job_worker_cd,=lcur.fetchone()

                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql=""
                            sql+=" UPDATE TEMP_ETL_JOB"
                            sql+=" SET job_worker_cd = :job_worker_cd"
                            sql+=" WHERE job_cd = :job_cd"
                            sql+=" AND grp_cd = :grp_cd"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql=""
                            sql+=" UPDATE TEMP_ETL_JOB"
                            sql+=" SET job_worker_cd = %(job_worker_cd)s"
                            sql+=" WHERE job_cd = %(job_cd)s"
                            sql+=" AND grp_cd = %(grp_cd)s"

                        ldic = {}
                        ldic['job_cd'] = lv_temp_job_cd
                        ldic['grp_cd'] = lv_temp_grp_cd
                        ldic['job_worker_cd']=lv_temp_job_worker_cd

                        self.log.Write(sql, ldic)
                        lcur.execute(sql,ldic)

                        self.log.Write("rowcount=",lcur.rowcount)

                        #assaing the worker into temp table
                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql=""
                            sql+=" UPDATE TEMP_ETL_WORKER_LOAD"
                            sql+=" SET job_cnt = job_cnt + 1"
                            sql+=" WHERE job_worker_cd = :job_worker_cd"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql=""
                            sql+=" UPDATE TEMP_ETL_WORKER_LOAD"
                            sql+=" SET job_cnt = job_cnt + 1"
                            sql+=" WHERE job_worker_cd = %(job_worker_cd)s"

                        ldic = {}
                        ldic['job_worker_cd']=lv_temp_job_worker_cd

                        self.log.Write(sql, ldic)
                        lcur.execute(sql,ldic)

                        self.log.Write("rowcount=",lcur.rowcount)

                    # End of Job Loop

                    # Assigning the workers for jobs from the temp table

                    sql=""
                    sql+=" UPDATE ETL_JOB j"
                    sql+=" SET job_worker_cd ="
                    sql+=" (SELECT j2.job_worker_cd"
                    sql+=" FROM TEMP_ETL_JOB j2"
                    sql+=" WHERE j.job_cd = j2.job_cd"
                    sql+=" AND j.grp_cd = j2.grp_cd)"
                    sql+=" ,last_run_status = 'C'"
                    sql+=" WHERE EXISTS"
                    sql+= " (SELECT j2.job_worker_cd"
                    sql+= " FROM TEMP_ETL_JOB j2"
                    sql+= " WHERE j.job_cd = j2.job_cd"
                    sql+= " AND j.grp_cd = j2.grp_cd)"
                    self.log.Write(sql)
                    lcur.execute(sql)
                    self.logdb.lcon.commit()
                    self.log.Write("rowcount=", lcur.rowcount)

                    lv_job_agent_end_datetime = datetime.datetime.now()
                    lv_job_agent_run_duration = dt.DateDiff(lv_job_agent_start_datetime, lv_job_agent_end_datetime)
                    lv_job_agent_next_run_datetime=dt.NextRunDate(lv_job_agent_next_run_datetime,lv_job_agent_sch_cd)
                    self.log.Write("lv_job_agent_end_datetime=",lv_job_agent_end_datetime)

                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql =""
                        sql+=" UPDATE etl_job_agent"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt =:job_agent_end_datetime,"
                        sql+=" last_run_duration =:job_agent_run_duration,"
                        sql+=" next_run_dt =:job_agent_next_run_datetime"
                        sql+=" WHERE job_agent_cd= :job_agent_cd"
                        sql+=" AND record_status = 'A'"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql =""
                        sql+=" UPDATE etl_job_agent"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt =%(job_agent_end_datetime)s,"
                        sql+=" last_run_duration =%(job_agent_run_duration)s,"
                        sql+=" next_run_dt =%(job_agent_next_run_datetime)s"
                        sql+=" WHERE job_agent_cd= %(job_agent_cd)s"
                        sql+=" AND record_status = 'A'"

                    ldic = {}
                    ldic['job_agent_cd'] = self.cv_job_agent_cd
                    ldic['job_agent_run_duration'] = lv_job_agent_run_duration
                    ldic['job_agent_end_datetime'] = lv_job_agent_end_datetime
                    ldic['job_agent_next_run_datetime'] = lv_job_agent_next_run_datetime

                    self.log.Write(sql, ldic)
                    lcur.execute(sql, ldic)
                    self.logdb.lcon.commit()
                    self.log.Write("rowcount=",lcur.rowcount)


                else: # If there is no Jobs scheduled for Agent

                    lv_job_agent_end_datetime = datetime.datetime.now()
                    lv_job_agent_run_duration = dt.DateDiff(lv_job_agent_start_datetime, lv_job_agent_end_datetime)
                    lv_job_agent_next_run_datetime=dt.NextRunDate(lv_job_agent_next_run_datetime,lv_job_agent_sch_cd)
                    self.log.Write("lv_job_agent_end_datetime=",lv_job_agent_end_datetime)
                    self.log.Write("lv_job_agent_next_run_datetime=",lv_job_agent_next_run_datetime)

                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql =""
                        sql+=" UPDATE etl_job_agent"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt =:job_agent_end_datetime,"
                        sql+=" last_run_duration =:job_agent_run_duration,"
                        sql+=" next_run_dt =:job_agent_next_run_datetime"
                        sql+=" WHERE job_agent_cd= :job_agent_cd"
                        sql+=" AND record_status = 'A'"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql =""
                        sql+=" UPDATE etl_job_agent"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt =%(job_agent_end_datetime)s,"
                        sql+=" last_run_duration =%(job_agent_run_duration)s,"
                        sql+=" next_run_dt =%(job_agent_next_run_datetime)s"
                        sql+=" WHERE job_agent_cd= %(job_agent_cd)s"
                        sql+=" AND record_status = 'A'"

                    lvdic = {}
                    lvdic['job_agent_cd'] = self.cv_job_agent_cd
                    lvdic['job_agent_run_duration'] = lv_job_agent_run_duration
                    lvdic['job_agent_end_datetime'] = lv_job_agent_end_datetime
                    lvdic['job_agent_next_run_datetime'] = lv_job_agent_next_run_datetime

                    self.log.Write(sql, lvdic)
                    lcur.execute(sql, lvdic)
                    self.logdb.lcon.commit()
                    self.log.Write("rowcount=", lcur.rowcount)

            else: # If Agent is in "Running" Mode
                x=None

            lcur.close()
            self.logdb.lcon.commit()

        except BaseException as e:
            print(e)
            self.log.Write(e)
            self.log.Write(traceback.print_exc())

            # If there is no Jobs scheduled for Agent
            lv_job_agent_end_datetime = datetime.datetime.now()
            lv_job_agent_run_duration = dt.DateDiff(lv_job_agent_start_datetime, lv_job_agent_end_datetime)
            lv_job_agent_next_run_datetime = dt.NextRunDate(lv_job_agent_next_run_datetime, lv_job_agent_sch_cd)

            if self.p.getParameter('etl_db.type').lower() == "oracle":
                sql = ""
                sql += " UPDATE etl_job_agent"
                sql += " SET"
                sql += " last_run_status = 'F',"
                sql += " last_run_end_dt =:job_agent_end_datetime,"
                sql += " last_run_duration =:job_agent_run_duration,"
                sql += " next_run_dt =:job_agent_next_run_datetime"
                sql += " WHERE job_agent_cd= :job_agent_cd"
                sql += " AND record_status = 'A'"
            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                sql = ""
                sql += " UPDATE etl_job_agent"
                sql += " SET"
                sql += " last_run_status = 'F',"
                sql += " last_run_end_dt =%(job_agent_end_datetime)s,"
                sql += " last_run_duration =%(job_agent_run_duration)s,"
                sql += " next_run_dt =%(job_agent_next_run_datetime)s"
                sql += " WHERE job_agent_cd= %(job_agent_cd)s"
                sql += " AND record_status = 'A'"

            lvdic = {}
            lvdic['job_agent_cd'] = self.cv_job_agent_cd
            lvdic['job_worker_run_duration'] = lv_job_agent_run_duration
            lvdic['job_worker_end_datetime'] = lv_job_agent_end_datetime
            lvdic['job_worker_next_run_datetime'] = lv_job_agent_next_run_datetime

            self.log.Write(sql, lvdic)
            lcur.execute(sql, lvdic)
            self.logdb.lcon.commit()
            self.log.Write("rowcount=", lcur.rowcount)

            if lcur:
                lcur.close()
            if lcur2:
                lcur2.close()
            if lcon:
                self.logdb.lcon.rollback()
        finally:
            self.logdb.CloseLogDBConnection()
            self.log.Write("End: Agent Execute")
