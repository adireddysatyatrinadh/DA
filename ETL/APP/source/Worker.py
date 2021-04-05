from Parameter import Parameter
from File import File
from DB import DB
from LogDB import LogDB
from Date import Date
from Job import Job
import sys
import datetime
import os
import traceback


class Worker:

    def __init__(self,p_JobWorkerCd,pLog):
        self.cv_job_worker_cd = p_JobWorkerCd
        self.log = pLog

        self.log.Write("BEGIN: Worker Constructor")
        self.logdb = LogDB(self.log)
        self.logdb.OpenLogDBConnection()
        self.cv_job_agent_cd = self.logdb.getJobAgentCd(self.cv_job_worker_cd)
        self.cv_job_agent_grp_cd = self.logdb.getJobAgentGrpCd(self.cv_job_agent_cd)

        self.p = Parameter(self.cv_job_agent_cd,self.log)
        self.wdb = DB(self.p, self.log)
        self.wdb.OpenSourceDBConnection()
        self.wdb.OpenDestinationDBConnection()
        self.wdb.AuthenticateDestinationServer()
        self.log.Write("END: Worker Constructor")

    def Execute(self):
        try:
            self.log.Write("Begin: Worker Execute")
            lv_job_worker_start_datetime=datetime.datetime.now()
            lcurj = ""
            lcurjs = ""
            lcur = ""

            dt = Date(self.log)

            lcur = self.logdb.lcon.cursor()

            if self.p.getParameter('etl_db.type').lower() == "oracle":
                sql = ""
                sql += " UPDATE etl_job_worker jw"
                sql += " SET"
                sql += " last_run_status = 'R'"
                sql += " ,last_run_start_dt = :job_worker_start_datetime"
                sql += " ,last_run_no = COALESCE(last_run_no,0)+1"
                sql += " WHERE jw.job_worker_cd = :job_worker_cd"
                sql += " AND jw.record_status = 'A'"
                sql += " AND jw.last_run_status NOT IN('R')"
            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                sql = ""
                sql += " UPDATE etl_job_worker jw"
                sql += " SET"
                sql += " last_run_status = 'R'"
                sql += " ,last_run_start_dt = %(job_worker_start_datetime)s"
                sql += " ,last_run_no = COALESCE(last_run_no,0)+1"
                sql += " WHERE jw.job_worker_cd = %(job_worker_cd)s"
                sql += " AND jw.record_status = 'A'"
                sql += " AND jw.last_run_status NOT IN('R')"

            self.log.Write(sql)
            ldic={}
            ldic['job_worker_cd'] = self.cv_job_worker_cd
            ldic['job_worker_start_datetime'] = lv_job_worker_start_datetime
            self.log.Write(ldic)
            lcur.execute(sql,ldic)
            self.logdb.lcon.commit()
            self.log.Write("rowcount=", lcur.rowcount)

            if (lcur.rowcount > 0):

                #   Fetch Next Job Run No
                if self.p.getParameter('etl_db.type').lower() == "oracle":
                    sql = ""
                    sql += " SELECT last_run_no"
                    sql += " FROM etl_job_worker jw"
                    sql += " WHERE jw.job_worker_cd = :job_worker_cd"
                    sql += " AND jw.record_status = 'A'"
                elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                    sql = ""
                    sql += " SELECT last_run_no"
                    sql += " FROM etl_job_worker jw"
                    sql += " WHERE jw.job_worker_cd = %(job_worker_cd)s"
                    sql += " AND jw.record_status = 'A'"

                ldic={}
                ldic['job_worker_cd'] =self.cv_job_worker_cd
                self.log.Write(sql,ldic)
                lcur.execute(sql,ldic)
                self.log.Write("rowcount=", lcur.rowcount)

                lv_job_worker_run_no, = lcur.fetchone()
                self.log.Write("job_worker_run_no=job_run_no=",lv_job_worker_run_no)

                #   Fetch Worker Schedule Code
                if self.p.getParameter('etl_db.type').lower() == "oracle":
                    sql = ""
                    sql += " SELECT next_run_dt,sch_cd"
                    sql += " FROM etl_job_worker"
                    sql += " WHERE job_worker_cd = :job_worker_cd"
                elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                    sql = ""
                    sql += " SELECT next_run_dt,sch_cd"
                    sql += " FROM etl_job_worker"
                    sql += " WHERE job_worker_cd = %(job_worker_cd)s"

                ldic={}
                ldic['job_worker_cd'] =self.cv_job_worker_cd
                self.log.Write(sql,ldic)
                lcur.execute(sql, ldic)
                self.log.Write("rowcount=",lcur.rowcount)

                lv_job_worker_next_run_datetime,lv_job_worker_sch_cd = lcur.fetchone()
                self.log.Write(lv_job_worker_next_run_datetime,lv_job_worker_sch_cd)

                lv_job_start_datetime = datetime.datetime.now()
                # making the run status into Que against the worker
                if self.p.getParameter('etl_db.type').lower() == "oracle":
                    sql = ""
                    sql += " UPDATE etl_job j"
                    sql += " SET "
                    sql += " last_run_status = 'Q'"
                    sql += " ,last_run_start_dt = :job_start_datetime"
                    sql += " ,last_run_no = :job_run_no"
                    sql += " WHERE j.job_worker_cd = :job_worker_cd"
                    sql += " AND j.record_status = 'A'"
                    sql += " AND j.last_run_status ='C'"
                elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                    sql =""
                    sql += " UPDATE etl_job j"
                    sql += " SET "
                    sql += " last_run_status = 'Q'"
                    sql += " ,last_run_start_dt = %(job_start_datetime)s"
                    sql += " ,last_run_no = %(job_run_no)s"
                    sql += " WHERE j.job_worker_cd = %(job_worker_cd)s"
                    sql += " AND j.record_status = 'A'"
                    sql += " AND j.last_run_status ='C'"

                self.log.Write(sql)
                ldic={}
                ldic['job_worker_cd'] = self.cv_job_worker_cd
                ldic['job_run_no'] = lv_job_worker_run_no
                ldic['job_start_datetime'] = lv_job_start_datetime

                self.log.Write(ldic)
                lcur.execute(sql,ldic)
                self.log.Write("rowcount=",lcur.rowcount)
                self.logdb.lcon.commit()

                lv_create_dt=datetime.datetime.now()
                lv_create_by=self.cv_job_worker_cd
                if (lcur.rowcount > 0):


                    # maintaining the job log ffromdtor every run
                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql = ""
                        sql += " INSERT INTO etl_job_log(job_log_id,job_cd,job_run_no,run_status,"
                        sql += " org_cd,loc_cd,grp_cd,record_status,run_start_dt,CREATE_BY,CREATE_DT)"
                        sql += " SELECT seq_etl_job_log__id.NEXTVAL,job_cd,last_run_no,last_run_status,"
                        sql += " loc_cd,org_cd,grp_cd,record_status,:job_start_datetime,:create_by,:create_dt"
                        sql += " FROM etl_job j"
                        sql += " WHERE job_worker_cd = :job_worker_cd "
                        sql += " AND j.record_status = 'A'"
                        sql += " AND j.last_run_status IN('Q')"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql = ""
                        sql += " INSERT INTO etl_job_log(job_log_id,job_cd,job_run_no,run_status,"
                        sql += " org_cd,loc_cd,grp_cd,record_status,run_start_dt,CREATE_BY,CREATE_DT)"
                        sql += " SELECT NEXTVAL('seq_etl_job_log__id'),job_cd,last_run_no,last_run_status,"
                        sql += " loc_cd,org_cd,grp_cd,record_status,%(job_start_datetime)s,%(create_by)s,%(create_dt)s"
                        sql += " FROM etl_job j"
                        sql += " WHERE job_worker_cd = %(job_worker_cd)s "
                        sql += " AND j.record_status = 'A'"
                        sql += " AND j.last_run_status IN('Q')"

                    ldic={}
                    ldic['job_worker_cd'] = self.cv_job_worker_cd
                    ldic['job_start_datetime'] = lv_job_start_datetime
                    ldic['create_by'] = lv_create_by
                    ldic['create_dt'] = lv_create_dt
                    self.log.Write(sql,ldic)
                    lcur.execute(sql,ldic)
                    self.log.Write("rowcount=",lcur.rowcount)
                    self.logdb.lcon.commit()

                    lv_is_job_failed = False

                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql =""
                        sql+=" SELECT job_cd, job_group_cd,next_run_dt,last_success_run_dt,loc_cd,org_cd,grp_cd"
                        sql+=" FROM etl_job j "
                        sql+=" WHERE job_worker_cd = :job_worker_cd"
                        sql+=" AND j.record_status = 'A'"
                        sql+=" AND j.last_run_status IN('Q')"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql =""
                        sql+=" SELECT job_cd, job_group_cd,next_run_dt,last_success_run_dt,loc_cd,org_cd,grp_cd"
                        sql+=" FROM etl_job j "
                        sql+=" WHERE job_worker_cd = %(job_worker_cd)s"
                        sql+=" AND j.record_status = 'A'"
                        sql+=" AND j.last_run_status IN('Q')"

                    ldic={}
                    ldic['job_worker_cd'] =self.cv_job_worker_cd
                    self.log.Write(sql,ldic)
                    lcurj = self.logdb.lcon.cursor()
                    lcurj.execute(sql,ldic)
                    self.log.Write("rowcount=",lcur.rowcount)
                    while True:
                        # Execute Each and Every Job
                        self.log.Write("Begin: Job")
                        lrowj=lcurj.fetchone()
                        if lrowj is None:
                            break
                        try:
                            lv_job_start_datetime = datetime.datetime.now()
                            self.log.Write("job_start_datetime=", lv_job_start_datetime)

                            lv_job_cd,lv_job_group_cd,lv_job_next_run_datetime,lv_job_last_success_run_dt,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd=lrowj
                            # making run last run status into RUNNING against the selected job
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job"
                                sql+=" SET last_run_status = 'R'"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" UPDATE etl_job"
                                sql+=" SET last_run_status = 'R'"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"

                            ldic={}
                            ldic['job_cd'] = lv_job_cd
                            ldic['grp_cd'] =lv_job_grp_cd

                            self.log.Write(sql,ldic)
                            lcur.execute(sql, ldic)
                            self.log.Write("rowcount=",lcur.rowcount)
                            self.logdb.lcon.commit()

                            #making run last run status into RUNNING against the selected job log
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job_log"
                                sql+=" SET run_status = 'R'"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                                sql+=" AND job_run_no = :job_run_no"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" UPDATE etl_job_log"
                                sql+=" SET run_status = 'R'"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"
                                sql+=" AND job_run_no = %(job_run_no)s"

                            ldic={}
                            ldic['job_cd'] = lv_job_cd
                            ldic['grp_cd'] =lv_job_grp_cd
                            ldic['job_run_no'] = lv_job_worker_run_no

                            self.log.Write(sql,ldic)
                            lcur.execute(sql, ldic)
                            self.log.Write("rowcount=",lcur.rowcount)
                            self.logdb.lcon.commit()
                            #making run last run status into Queue against the selected job step
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job_step"
                                sql+=" SET last_run_status = 'Q'"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" UPDATE etl_job_step"
                                sql+=" SET last_run_status = 'Q'"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"

                            ldic={}
                            ldic['job_cd'] = lv_job_cd
                            ldic['grp_cd'] = lv_job_grp_cd

                            self.log.Write(sql,ldic)
                            lcur.execute(sql,ldic )
                            self.log.Write("rowcount=",lcur.rowcount)
                            self.logdb.lcon.commit()

                            lv_is_job_step_failed = False
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" SELECT job_log_id"
                                sql+=" FROM etl_job_log"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                                sql+=" AND job_run_no = :job_run_no"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" SELECT job_log_id"
                                sql+=" FROM etl_job_log"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"
                                sql+=" AND job_run_no = %(job_run_no)s"

                            ldic={}
                            ldic['job_cd'] =lv_job_cd
                            ldic['grp_cd'] =lv_job_grp_cd
                            ldic['job_run_no'] =lv_job_worker_run_no
                            self.log.Write(sql,ldic)
                            lcur.execute(sql, ldic)
                            self.log.Write("rowcount=",lcur.rowcount)
                            lv_job_log_id,=lcur.fetchone()

                            lv_create_dt = datetime.datetime.now()
                            # Maintaining the job step log for every run
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" INSERT INTO etl_job_step_log"
                                sql+=" (job_step_log_id, job_log_id, job_cd,job_run_no,job_step_cd,run_start_dt,run_status,"
                                sql+=" loc_cd, org_cd, grp_cd,record_status,create_by,create_dt)"
                                sql+=" SELECT"
                                sql+=" seq_etl_job_step_log__id.NEXTVAL,:job_log_id,j.job_cd,j.last_run_no,js.job_step_cd,:create_dt,js.last_run_status,"
                                sql+=" js.loc_cd, js.org_cd,js.grp_cd,js.record_status,:create_by,:create_dt"
                                sql+=" FROM etl_job_step js"
                                sql+=" INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                                sql+=" WHERE js.job_cd = :job_cd"
                                sql+=" AND js.grp_cd = :grp_cd"
                                sql+=" AND js.record_status = 'A'"
                                sql+=" AND js.last_run_status IN('Q')"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" INSERT INTO etl_job_step_log"
                                sql+=" (job_step_log_id, job_log_id, job_cd,job_run_no,job_step_cd,run_start_dt,run_status,"
                                sql+=" loc_cd, org_cd, grp_cd,record_status,create_by,create_dt)"
                                sql+=" SELECT"
                                sql+=" NEXTVAL('seq_etl_job_step_log__id'),%(job_log_id)s,j.job_cd,j.last_run_no,js.job_step_cd,%(create_dt)s,js.last_run_status,"
                                sql+=" js.loc_cd, js.org_cd,js.grp_cd,js.record_status,%(create_by)s,%(create_dt)s"
                                sql+=" FROM etl_job_step js"
                                sql+=" INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                                sql+=" WHERE js.job_cd = %(job_cd)s"
                                sql+=" AND js.grp_cd = %(grp_cd)s"
                                sql+=" AND js.record_status = 'A'"
                                sql+=" AND js.last_run_status IN('Q')"

                            ldic={}
                            ldic['job_cd'] = lv_job_cd
                            ldic['grp_cd'] = lv_job_grp_cd
                            ldic['job_log_id'] = lv_job_log_id
                            ldic['create_by'] = lv_create_by
                            ldic['create_dt'] = lv_create_dt
                            self.log.Write(sql,ldic)
                            lcur.execute(sql, ldic)
                            self.log.Write("rowcount=",lcur.rowcount)
                            self.logdb.lcon.commit()

                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" SELECT "
                                sql+=" js.job_step_cd,js.job_program_cd,js.job_program_action"
                                sql+=",js.job_program_arg1,js.job_program_arg2,js.job_program_arg3"
                                sql+=",js.job_program_arg4,js.job_program_arg5,js.job_program_arg6"
                                sql+=",js.job_program_arg7,js.job_program_arg8,js.job_program_arg9"
                                sql+=",js.job_program_arg10"
                                sql+=" FROM etl_job_step js"
                                sql+=" INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                                sql+=" WHERE js.job_cd = :job_cd "
                                sql+=" AND js.grp_cd = :grp_cd "
                                sql+=" AND js.record_status = 'A'"
                                sql+=" AND js.last_run_status IN('Q')"
                                sql+=" ORDER BY job_step_seq"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" SELECT "
                                sql+=" js.job_step_cd,js.job_program_cd,js.job_program_action"
                                sql+=",js.job_program_arg1,js.job_program_arg2,js.job_program_arg3"
                                sql+=",js.job_program_arg4,js.job_program_arg5,js.job_program_arg6"
                                sql+=",js.job_program_arg7,js.job_program_arg8,js.job_program_arg9"
                                sql+=",js.job_program_arg10"
                                sql+=" FROM etl_job_step js"
                                sql+=" INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                                sql+=" WHERE js.job_cd = %(job_cd)s "
                                sql+=" AND js.grp_cd = %(grp_cd)s "
                                sql+=" AND js.record_status = 'A'"
                                sql+=" AND js.last_run_status IN('Q')"
                                sql+=" ORDER BY job_step_seq"

                            ldic={}
                            ldic['job_cd'] = lv_job_cd
                            ldic['grp_cd'] = lv_job_grp_cd
                            self.log.Write(sql,ldic)
                            lcurjs = self.logdb.lcon.cursor()
                            lcurjs.execute(sql,ldic)
                            self.log.Write("rowcount=",lcurjs.rowcount)

                            while True:
                                # Execute Each and Every Job Step for Current Job
                                self.log.Write("Begin: Job Step")
                                lrowjs = lcurjs.fetchone()
                                if lrowjs is None:
                                    break

                                try:
                                    self.log.Write(lrowjs)
                                    lv_job_step_start_datetime = datetime.datetime.now()
                                    self.log.Write("lv_job_step_start_datetime=",lv_job_step_start_datetime)
                                    #lv_job_step_cd,lv_job_program_cd,lv_job_program_action,lv_xml_file_name,lv_source_schema_name,lv_destination_schema_name,lv_grp_cd,lv_org_cd,lv_loc_cd=lrowjs
                                    lv_job_step_cd,lv_job_program_cd,lv_job_program_action,lv_job_program_arg1,lv_job_program_arg2,iv_job_program_arg3,lv_job_program_arg4,lv_job_program_arg5,lv_job_program_arg6,lv_job_program_arg7,lv_job_program_arg8,lv_job_program_arg9,lv_job_program_arg10=lrowjs
                                    # making run last run status into RUNNING against the selected job step
                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql=""
                                        sql+=" UPDATE etl_job_step"
                                        sql+=" SET"
                                        sql+=" last_run_status = 'R',"
                                        sql+=" last_run_start_dt = :job_step_start_datetime"
                                        sql+=" WHERE job_step_cd = :job_step_cd"
                                        sql+=" AND job_cd = :job_cd"
                                        sql+=" AND grp_cd = :grp_cd"
                                        sql+=" AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql=""
                                        sql+=" UPDATE etl_job_step"
                                        sql+=" SET"
                                        sql+=" last_run_status = 'R',"
                                        sql+=" last_run_start_dt = %(job_step_start_datetime)s"
                                        sql+=" WHERE job_step_cd = %(job_step_cd)s"
                                        sql+=" AND job_cd = %(job_cd)s"
                                        sql+=" AND grp_cd = %(grp_cd)s"
                                        sql+=" AND record_status = 'A'"

                                    ldic={}
                                    ldic['job_cd'] = lv_job_cd
                                    ldic['grp_cd'] = lv_job_grp_cd
                                    ldic['job_step_cd'] = lv_job_step_cd
                                    ldic['job_step_start_datetime'] = lv_job_step_start_datetime
                                    self.log.Write(sql,ldic)
                                    lcur.execute(sql,ldic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()
                                    #making run last run status into RUNNING against the selected job step log

                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql=""
                                        sql+=" UPDATE etl_job_step_log"
                                        sql+=" SET run_status = 'R'"
                                        sql+=" ,run_start_dt = :job_step_start_datetime"
                                        sql+=" WHERE job_step_cd = :job_step_cd"
                                        sql+=" AND job_cd = :job_cd"
                                        sql+=" AND job_run_no = :job_run_no"
                                        sql+=" AND grp_cd = :grp_cd"
                                        sql+=" AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql=""
                                        sql+=" UPDATE etl_job_step_log"
                                        sql+=" SET run_status = 'R'"
                                        sql+=" ,run_start_dt = %(job_step_start_datetime)s"
                                        sql+=" WHERE job_step_cd = %(job_step_cd)s"
                                        sql+=" AND job_cd = %(job_cd)s"
                                        sql+=" AND job_run_no = %(job_run_no)s"
                                        sql+=" AND grp_cd = %(grp_cd)s"
                                        sql+=" AND record_status = 'A'"

                                    ldic={}
                                    ldic['job_cd'] = lv_job_cd
                                    ldic['grp_cd'] = lv_job_grp_cd
                                    ldic['job_run_no'] = lv_job_worker_run_no
                                    ldic['job_step_cd'] = lv_job_step_cd
                                    ldic['job_step_start_datetime'] = lv_job_step_start_datetime
                                    self.log.Write(sql,ldic)
                                    lcur.execute(sql, ldic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()

                                    # CALL Job.Execute Method
                                    self.log.Write("BEGIN: Job.Execute Call")

                                    self.log.Write("INPUT:")
                                    self.log.Write(self.log)
                                    self.log.Write(self.wdb)
                                    self.log.Write(self.logdb)
                                    self.log.Write(self.p)
                                    self.log.Write(self.cv_job_worker_cd, lv_job_cd, lv_job_step_cd, lv_job_grp_cd)

                                    self.log.Write("Creating Job Object")
                                    j=Job(self.log,self.wdb,self.logdb,self.p)
                                    self.log.Write("Before Job.Execute Method")
                                    jobStepStatus, errorCd, errorMsg=j.Execute(self.cv_job_worker_cd,lv_job_cd,lv_job_step_cd,lv_job_grp_cd)
                                    self.log.Write("After: Job.Execute Method")

                                    self.log.Write("OUTPUT:")
                                    self.log.Write(jobStepStatus, errorCd, errorMsg)

                                    self.log.Write("END: Job.Execute Call")

                                    if (jobStepStatus) != 0:
                                        lv_is_job_step_failed = True
                                        lv_is_job_failed = True

                                except BaseException as e:
                                    self.log.Write(traceback.print_exc())
                                    lv_is_job_step_failed = True
                                    lv_is_job_failed = True
                                    errorMsg = str(e)
                                    errorCd = str(e)
                                    jobStepStatus = 1
                                finally:
                                    lv_job_step_end_datetime = datetime.datetime.now()
                                    lv_job_step_run_duration = dt.DateDiff(lv_job_step_start_datetime,lv_job_step_end_datetime)
                                    self.log.Write("lv_job_step_end_datetime=",lv_job_step_end_datetime)
                                    self.log.Write("lv_job_step_run_duration=",lv_job_step_run_duration)

                                if (not lv_is_job_step_failed ):
                                    # If Job Step Successed
                                    # if status is sucess, making run status S against the job_step
                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql = ""
                                        sql+=" UPDATE etl_job_step"
                                        sql+=" SET"
                                        sql+=" last_run_status = 'S',"
                                        sql+=" last_run_end_dt = :job_step_end_datetime,"
                                        sql+=" last_success_run_dt =:job_worker_start_datetime,"
                                        sql+=" last_run_duration =:job_step_run_duration"
                                        sql+=" WHERE job_step_cd = :job_step_cd"
                                        sql+=" AND job_cd = :job_cd"
                                        sql+=" AND grp_cd = :grp_cd"
                                        sql+=" AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql = ""
                                        sql+=" UPDATE etl_job_step"
                                        sql+=" SET"
                                        sql+=" last_run_status = 'S',"
                                        sql+=" last_run_end_dt = %(job_step_end_datetime)s,"
                                        sql+=" last_success_run_dt =%(job_worker_start_datetime)s,"
                                        sql+=" last_run_duration =%(job_step_run_duration)s"
                                        sql+=" WHERE job_step_cd = %(job_step_cd)s"
                                        sql+=" AND job_cd = %(job_cd)s"
                                        sql+=" AND grp_cd = %(grp_cd)s"
                                        sql+=" AND record_status = 'A'"

                                    lvdic={}
                                    lvdic['job_step_cd'] = lv_job_step_cd
                                    lvdic['job_cd'] =lv_job_cd
                                    lvdic['grp_cd'] =lv_job_grp_cd
                                    lvdic['job_step_run_duration'] =lv_job_step_run_duration
                                    lvdic['job_step_end_datetime'] = lv_job_step_end_datetime
                                    lvdic['job_worker_start_datetime'] =lv_job_worker_start_datetime

                                    self.log.Write(sql,lvdic)
                                    lcur.execute(sql, lvdic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()


                                    #if status is sucess, making run status S against the job_step_log
                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql=""
                                        sql+=" UPDATE etl_job_step_log"
                                        sql+=" SET run_status = 'S',"
                                        sql+=" run_end_dt = :job_step_end_datetime,"
                                        sql+=" run_duration =:job_step_run_duration,"
                                        sql+=" job_step_output = 'SUCCESS'"
                                        sql+=" WHERE job_step_cd = :job_step_cd"
                                        sql+=" AND job_cd = :job_cd"
                                        sql+=" AND job_run_no = :job_run_no"
                                        sql+=" AND grp_cd = :grp_cd"
                                        sql+=" AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql=""
                                        sql+=" UPDATE etl_job_step_log"
                                        sql+=" SET run_status = 'S',"
                                        sql+=" run_end_dt = %(job_step_end_datetime)s,"
                                        sql+=" run_duration =%(job_step_run_duration)s,"
                                        sql+=" job_step_output = 'SUCCESS'"
                                        sql+=" WHERE job_step_cd = %(job_step_cd)s"
                                        sql+=" AND job_cd = %(job_cd)s"
                                        sql+=" AND job_run_no = %(job_run_no)s"
                                        sql+=" AND grp_cd = %(grp_cd)s"
                                        sql+=" AND record_status = 'A'"

                                    lvdic={}
                                    lvdic['job_step_cd'] = lv_job_step_cd
                                    lvdic['job_cd'] =lv_job_cd
                                    lvdic['grp_cd'] =lv_job_grp_cd
                                    lvdic['job_run_no'] = lv_job_worker_run_no
                                    lvdic['job_step_run_duration'] =lv_job_step_run_duration
                                    lvdic['job_step_end_datetime'] = lv_job_step_end_datetime

                                    self.log.Write(sql,lvdic)
                                    lcur.execute(sql, lvdic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()
                                else: # If Job Step Fails

                                    # if status is fail, making run status F against the job_step
                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql = ""
                                        sql += " UPDATE etl_job_step"
                                        sql += " SET"
                                        sql += " last_run_status = 'F',"
                                        sql += " last_run_end_dt = :job_step_end_datetime,"
                                        sql += " last_run_duration =:job_step_run_duration"
                                        sql += " WHERE job_step_cd = :job_step_cd"
                                        sql += " AND job_cd = :job_cd"
                                        sql += " AND grp_cd = :grp_cd"
                                        sql += " AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql = ""
                                        sql += " UPDATE etl_job_step"
                                        sql += " SET"
                                        sql += " last_run_status = 'F',"
                                        sql += " last_run_end_dt = %(job_step_end_datetime)s,"
                                        sql += " last_run_duration = %(job_step_run_duration)s"
                                        sql += " WHERE job_step_cd = %(job_step_cd)s"
                                        sql += " AND job_cd = %(job_cd)s"
                                        sql += " AND grp_cd = %(grp_cd)s"
                                        sql += " AND record_status = 'A'"

                                    lvdic = {}
                                    lvdic['job_step_cd'] = lv_job_step_cd
                                    lvdic['job_cd'] = lv_job_cd
                                    lvdic['grp_cd'] = lv_job_grp_cd
                                    lvdic['job_step_run_duration'] = lv_job_step_run_duration
                                    lvdic['job_step_end_datetime'] = lv_job_step_end_datetime

                                    self.log.Write(sql,lvdic)
                                    lcur.execute(sql, lvdic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()

                                    # if status is sucess, making run status S against the job_step_log
                                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                                        sql = ""
                                        sql += " UPDATE etl_job_step_log"
                                        sql += " SET run_status = 'F',"
                                        sql += " run_end_dt = :job_step_end_datetime,"
                                        sql += " run_duration =:job_step_run_duration,"
                                        sql += " job_step_output ='FAILED',"
                                        sql += " job_step_error_message =:job_step_error_message"
                                        sql += " WHERE job_step_cd = :job_step_cd"
                                        sql += " AND job_cd = :job_cd"
                                        sql += " AND job_run_no = :job_run_no"
                                        sql += " AND grp_cd = :grp_cd"
                                        sql += " AND record_status = 'A'"
                                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                        sql = ""
                                        sql += " UPDATE etl_job_step_log"
                                        sql += " SET run_status = 'F',"
                                        sql += " run_end_dt = %(job_step_end_datetime)s,"
                                        sql += " run_duration =%(job_step_run_duration)s,"
                                        sql += " job_step_output ='FAILED',"
                                        sql += " job_step_error_message =%(job_step_error_message)s"
                                        sql += " WHERE job_step_cd = %(job_step_cd)s"
                                        sql += " AND job_cd = %(job_cd)s"
                                        sql += " AND job_run_no = %(job_run_no)s"
                                        sql += " AND grp_cd = %(grp_cd)s"
                                        sql += " AND record_status = 'A'"

                                    lvdic = {}
                                    lvdic['job_step_cd'] = lv_job_step_cd
                                    lvdic['job_cd'] = lv_job_cd
                                    lvdic['grp_cd'] = lv_job_grp_cd
                                    lvdic['job_run_no'] = lv_job_worker_run_no
                                    lvdic['job_step_run_duration'] = lv_job_step_run_duration
                                    lvdic['job_step_end_datetime'] = lv_job_step_end_datetime
                                    lvdic['job_step_error_message'] =errorMsg
                                    self.log.Write(sql,lvdic)
                                    lcur.execute(sql, lvdic)
                                    self.log.Write("rowcount=", lcur.rowcount)
                                    self.logdb.lcon.commit()
                                #End : if jobStepStatus=0

                                self.log.Write("End: Job Step")

                            #End Job Step :
                            lcurjs.close()

                        except BaseException as e:
                            self.log.Write(traceback.print_exc())
                            lv_is_job_failed = True
                            errorMsg=str(e)
                            self.log.Write("errorMsg=",errorMsg)
                        finally:
                            lv_job_end_datetime=datetime.datetime.now()
                            lv_job_run_duration = dt.DateDiff(lv_job_start_datetime, lv_job_end_datetime)
                            self.log.Write("job_end_datetime=",lv_job_end_datetime)
                            self.log.Write("lv_job_run_duration=",lv_job_run_duration)

                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql = ""
                            sql += " SELECT sch_cd"
                            sql += " FROM etl_job_schedule js "
                            sql += " WHERE job_cd = :job_cd"
                            sql += " AND grp_cd = :grp_cd"
                            sql += " AND record_status = 'A'"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql = ""
                            sql += " SELECT sch_cd"
                            sql += " FROM etl_job_schedule js "
                            sql += " WHERE job_cd = %(job_cd)s"
                            sql += " AND grp_cd = %(grp_cd)s"
                            sql += " AND record_status = 'A'"

                        lvdic = {}
                        lvdic['job_cd'] = lv_job_cd
                        lvdic['grp_cd'] = lv_job_grp_cd
                        self.log.Write(sql, lvdic)
                        lcur.execute(sql, lvdic)
                        self.log.Write("rowcount=", lcur.rowcount)
                        lv_job_sch_cd, = lcur.fetchone()

                        lv_job_next_run_datetime=dt.NextRunDate(lv_job_next_run_datetime,lv_job_sch_cd)

                        if (lv_is_job_failed):

                            #any step failed, total job status put into fail
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job"
                                sql+=" SET"
                                sql+=" last_run_status = 'F',"
                                sql+=" last_run_end_dt = :job_end_datetime,"
                                sql+=" last_run_duration =:job_run_duration,"
                                sql+=" next_run_dt =:job_next_run_dt"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                 sql = ""
                                 sql += " UPDATE etl_job"
                                 sql += " SET"
                                 sql += " last_run_status = 'F',"
                                 sql += " last_run_end_dt = %(job_end_datetime)s,"
                                 sql += " last_run_duration = %(job_run_duration)s,"
                                 sql += " next_run_dt = %(job_next_run_dt)s"
                                 sql += " WHERE job_cd = %(job_cd)s"
                                 sql += " AND grp_cd = %(grp_cd)s"
                                 sql += " AND record_status = 'A'"

                            lvdic = {}
                            lvdic['job_cd'] = lv_job_cd
                            lvdic['grp_cd'] = lv_job_grp_cd
                            lvdic['job_run_duration'] = lv_job_run_duration
                            lvdic['job_end_datetime'] = lv_job_end_datetime
                            lvdic['job_next_run_dt'] = lv_job_next_run_datetime

                            self.log.Write(sql,lvdic)
                            lcur.execute(sql, lvdic)
                            self.log.Write("rowcount=", lcur.rowcount)
                            self.logdb.lcon.commit()

                            #any step failed, total job status put into fail
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job_log"
                                sql+=" SET"
                                sql+=" run_status = 'F',"
                                sql+=" run_end_dt = :job_end_datetime,"
                                sql+=" run_duration =:job_run_duration"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND job_run_no = :job_run_no"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql= ""
                                sql+=" UPDATE etl_job_log"
                                sql+=" SET"
                                sql+=" run_status = 'F',"
                                sql+=" run_end_dt = %(job_end_datetime)s,"
                                sql+=" run_duration = %(job_run_duration)s"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND job_run_no = %(job_run_no)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"

                            lvdic = {}
                            lvdic['job_cd'] = lv_job_cd
                            lvdic['grp_cd'] = lv_job_grp_cd
                            lvdic['job_run_no'] = lv_job_worker_run_no
                            lvdic['job_run_duration'] = lv_job_run_duration
                            lvdic['job_end_datetime'] = lv_job_end_datetime

                            self.log.Write(sql, lvdic)
                            lcur.execute(sql, lvdic)
                            self.log.Write("rowcount=", lcur.rowcount)
                            self.logdb.lcon.commit()
                        else:


                            # all steps are successed, total job status put into success
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql=""
                                sql+=" UPDATE etl_job"
                                sql+=" SET"
                                sql+=" last_run_status = 'S',"
                                sql+=" last_run_end_dt = :job_end_datetime,"
                                sql+=" last_success_run_dt = :job_success_run_datetime,"
                                sql+=" last_run_duration = :job_run_duration,"
                                sql+=" next_run_dt =:job_next_run_dt"
                                sql+=" WHERE job_cd = :job_cd"
                                sql+=" AND grp_cd = :grp_cd"
                                sql+=" AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql=""
                                sql+=" UPDATE etl_job"
                                sql+=" SET"
                                sql+=" last_run_status = 'S',"
                                sql+=" last_run_end_dt = %(job_end_datetime)s,"
                                sql+=" last_success_run_dt = %(job_success_run_datetime)s,"
                                sql+=" last_run_duration = %(job_run_duration)s,"
                                sql+=" next_run_dt =%(job_next_run_dt)s"
                                sql+=" WHERE job_cd = %(job_cd)s"
                                sql+=" AND grp_cd = %(grp_cd)s"
                                sql+=" AND record_status = 'A'"

                            lvdic = {}
                            lvdic['job_cd'] = lv_job_cd
                            lvdic['grp_cd'] = lv_job_grp_cd
                            lvdic['job_run_duration'] = lv_job_run_duration
                            lvdic['job_end_datetime'] = lv_job_end_datetime
                            lvdic['job_success_run_datetime'] = lv_job_worker_start_datetime
                            lvdic['job_next_run_dt'] = lv_job_next_run_datetime
                            self.log.Write(sql,lvdic)
                            lcur.execute(sql, lvdic)
                            self.log.Write("rowcount=", lcur.rowcount)
                            self.logdb.lcon.commit()

                            # all steps are successed, total job status put into success
                            if self.p.getParameter('etl_db.type').lower() == "oracle":
                                sql = ""
                                sql += " UPDATE etl_job_log"
                                sql += " SET"
                                sql += " run_status = 'S',"
                                sql += " run_end_dt = :job_end_datetime,"
                                sql += " run_duration =:job_run_duration"
                                sql += " WHERE job_cd = :job_cd"
                                sql += " AND job_run_no = :job_run_no"
                                sql += " AND grp_cd = :grp_cd"
                                sql += " AND record_status = 'A'"
                            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                                sql = ""
                                sql += " UPDATE etl_job_log"
                                sql += " SET"
                                sql += " run_status = 'S',"
                                sql += " run_end_dt = %(job_end_datetime)s,"
                                sql += " run_duration =%(job_run_duration)s"
                                sql += " WHERE job_cd = %(job_cd)s"
                                sql += " AND job_run_no = %(job_run_no)s"
                                sql += " AND grp_cd = %(grp_cd)s"
                                sql += " AND record_status = 'A'"

                            lvdic = {}
                            lvdic['job_cd'] = lv_job_cd
                            lvdic['grp_cd'] = lv_job_grp_cd
                            lvdic['job_run_no'] = lv_job_worker_run_no
                            lvdic['job_run_duration'] = lv_job_run_duration
                            lvdic['job_end_datetime'] = lv_job_end_datetime
                            self.log.Write(sql,lvdic)
                            lcur.execute(sql, lvdic)
                            self.log.Write("rowcount=", lcur.rowcount)
                            self.logdb.lcon.commit()

                        #End Job:
                        self.log.Write("End: Job")

                    lcurj.close()


                    lv_job_worker_end_datetime = datetime.datetime.now()
                    lv_job_worker_run_duration = dt.DateDiff(lv_job_worker_start_datetime, lv_job_worker_end_datetime)
                    lv_job_worker_next_run_datetime=dt.NextRunDate(lv_job_worker_next_run_datetime,lv_job_worker_sch_cd)

                    if( lv_is_job_failed):
                        #any step failed, total worker job status put into fail

                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql=""
                            sql+=" UPDATE etl_job_worker jw"
                            sql+=" SET"
                            sql+=" last_run_status = 'F',"
                            sql+=" last_run_end_dt =:job_worker_end_datetime,"
                            sql+=" last_run_duration =:job_worker_run_duration,"
                            sql+=" next_run_dt =:job_worker_next_run_datetime"
                            sql+=" WHERE jw.job_worker_cd = :job_worker_cd"
                            sql+=" AND jw.record_status = 'A'"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql=""
                            sql+=" UPDATE etl_job_worker jw"
                            sql+=" SET"
                            sql+=" last_run_status = 'F',"
                            sql+=" last_run_end_dt = %(job_worker_end_datetime)s,"
                            sql+=" last_run_duration = %(job_worker_run_duration)s,"
                            sql+=" next_run_dt = %(job_worker_next_run_datetime)s"
                            sql+=" WHERE jw.job_worker_cd = %(job_worker_cd)s"
                            sql+=" AND jw.record_status = 'A'"

                        lvdic = {}

                        lvdic['job_worker_cd'] = self.cv_job_worker_cd
                        lvdic['job_worker_run_duration'] = lv_job_worker_run_duration
                        lvdic['job_worker_end_datetime'] = lv_job_worker_end_datetime
                        lvdic['job_worker_next_run_datetime'] = lv_job_worker_next_run_datetime
                        self.log.Write(sql, lvdic)
                        lcur.execute(sql, lvdic)
                        self.log.Write("rowcount=", lcur.rowcount)
                        self.logdb.lcon.commit()

                    else:
                        #all steps are successed, total worker job status put into success

                        if self.p.getParameter('etl_db.type').lower() == "oracle":
                            sql =""
                            sql+=" UPDATE etl_job_worker jw"
                            sql+=" SET"
                            sql+=" last_run_status = 'S',"
                            sql+=" last_run_end_dt =:job_worker_end_datetime,"
                            sql+=" last_run_duration =:job_worker_run_duration,"
                            sql+=" next_run_dt =:job_worker_next_run_datetime"
                            sql+=" WHERE jw.job_worker_cd = :job_worker_cd"
                            sql+=" AND jw.record_status = 'A'"
                        elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                            sql =""
                            sql+=" UPDATE etl_job_worker jw"
                            sql+=" SET"
                            sql+=" last_run_status = 'S',"
                            sql+=" last_run_end_dt =%(job_worker_end_datetime)s,"
                            sql+=" last_run_duration =%(job_worker_run_duration)s,"
                            sql+=" next_run_dt =%(job_worker_next_run_datetime)s"
                            sql+=" WHERE jw.job_worker_cd = %(job_worker_cd)s"
                            sql+=" AND jw.record_status = 'A'"

                        lvdic = {}
                        lvdic['job_worker_cd'] = self.cv_job_worker_cd
                        lvdic['job_worker_run_duration'] = lv_job_worker_run_duration
                        lvdic['job_worker_end_datetime'] = lv_job_worker_end_datetime
                        lvdic['job_worker_next_run_datetime'] = lv_job_worker_next_run_datetime

                        self.log.Write(sql, lvdic)
                        lcur.execute(sql, lvdic)
                        self.log.Write("rowcount=", lcur.rowcount)
                        self.logdb.lcon.commit()

                        self.log.Write("End: Job Worker")

                else:
                    # If there is no Jobs scheduled for Worker
                    lv_job_worker_end_datetime = datetime.datetime.now()
                    lv_job_worker_run_duration = dt.DateDiff(lv_job_worker_start_datetime, lv_job_worker_end_datetime)
                    lv_job_worker_next_run_datetime=dt.NextRunDate(lv_job_worker_next_run_datetime,lv_job_worker_sch_cd)

                    if self.p.getParameter('etl_db.type').lower() == "oracle":
                        sql=""
                        sql+=" UPDATE etl_job_worker jw"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt = :job_worker_end_datetime,"
                        sql+=" last_run_duration =:job_worker_run_duration,"
                        sql+=" next_run_dt =:job_worker_next_run_datetime"
                        sql+=" WHERE jw.job_worker_cd = :job_worker_cd"
                        sql+=" AND jw.record_status = 'A'"
                        sql+=" AND jw.last_run_status = 'R'"
                    elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                        sql=""
                        sql+=" UPDATE etl_job_worker jw"
                        sql+=" SET"
                        sql+=" last_run_status = 'S',"
                        sql+=" last_run_end_dt = %(job_worker_end_datetime)s,"
                        sql+=" last_run_duration =%(job_worker_run_duration)s,"
                        sql+=" next_run_dt =%(job_worker_next_run_datetime)s"
                        sql+=" WHERE jw.job_worker_cd = %(job_worker_cd)s"
                        sql+=" AND jw.record_status = 'A'"
                        sql+=" AND jw.last_run_status = 'R'"

                    lvdic = {}
                    lvdic['job_worker_cd'] = self.cv_job_worker_cd
                    lvdic['job_worker_run_duration'] = lv_job_worker_run_duration
                    lvdic['job_worker_end_datetime'] = lv_job_worker_end_datetime
                    lvdic['job_worker_next_run_datetime'] = lv_job_worker_next_run_datetime

                    self.log.Write(sql, lvdic)
                    lcur.execute(sql, lvdic)
                    self.log.Write("rowcount=", lcur.rowcount)
                    self.logdb.lcon.commit()

            else: # If Worker is in "Running" Mode
                x=None

            lcur.close()
            self.logdb.lcon.commit()

        except BaseException as e:
            self.log.Write(traceback.print_exc())
            self.log.Write("Begin: BaseException Block")
            if self.logdb.lcon:
                self.logdb.lcon.rollback()

            if lcurj:
                lcurj.close()
            if lcurjs:
                lcurjs.close()

            #self.log.Write(e.__traceback__)
            self.log.Write(traceback.print_exc())
            self.logdb.lcon.rollback()

            lv_job_worker_end_datetime = datetime.datetime.now()
            lv_job_worker_run_duration = dt.DateDiff(lv_job_worker_start_datetime, lv_job_worker_end_datetime)
            lv_job_worker_next_run_datetime = dt.NextRunDate(lv_job_worker_next_run_datetime, lv_job_worker_sch_cd)

            if self.p.getParameter('etl_db.type').lower() == "oracle":
                sql=""
                sql+=" UPDATE etl_job_worker jw"
                sql+=" SET"
                sql+=" last_run_status = 'F',"
                sql+=" last_run_end_dt =:job_worker_end_datetime,"
                sql+=" last_run_duration =:job_worker_run_duration,"
                sql+=" next_run_dt =:job_worker_next_run_datetime"
                sql+=" WHERE jw.job_worker_cd = :job_worker_cd"
                sql+=" AND jw.record_status = 'A'"
            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                sql=""
                sql+=" UPDATE etl_job_worker jw"
                sql+=" SET"
                sql+=" last_run_status = 'F',"
                sql+=" last_run_end_dt =%(job_worker_end_datetime)s,"
                sql+=" last_run_duration =%(job_worker_run_duration)s,"
                sql+=" next_run_dt =%(job_worker_next_run_datetime)s"
                sql+=" WHERE jw.job_worker_cd = %(job_worker_cd)s"
                sql+=" AND jw.record_status = 'A'"

            lvdic = {}
            lvdic['job_worker_cd'] = self.cv_job_worker_cd
            lvdic['job_worker_run_duration'] = lv_job_worker_run_duration
            lvdic['job_worker_end_datetime'] = lv_job_worker_end_datetime
            lvdic['job_worker_next_run_datetime'] = lv_job_worker_next_run_datetime

            self.log.Write(sql, lvdic)
            lcur.execute(sql, lvdic)
            self.log.Write("rowcount=", lcur.rowcount)
            self.logdb.lcon.commit()
            self.log.Write(lcur.rowcount)
            self.log.Write("End: BaseException Block")

        finally:
            self.log.Write("Begin:Finally Block")

            self.logdb.CloseLogDBConnection()
            self.wdb.CloseSourceDBConnection()
            self.wdb.CloseDestinationDBConnection()

            self.log.Write("End: Worker Execute")
