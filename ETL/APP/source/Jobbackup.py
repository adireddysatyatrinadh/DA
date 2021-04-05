from File import File
from Date import Date
from Table2Table import Table2Table
from Table2Index import Table2Index
from Table2File import Table2File
from File2Table import File2Table
from Parameter import Parameter
import sys
import datetime
import os
import traceback

class Job:

    def __init__(self,pLog,pWDB,pLogDB,pParameter):
        self.log=pLog
        self.log.Write("BEGIN: Job Constructor")
        self.wdb=pWDB
        self.logdb=pLogDB
        self.p = pParameter
        self.log.Write("END: Job Constructor")

    def Execute(self,p_job_worker_cd,p_job_cd,p_job_step_cd,p_job_grp_cd):
        try:
            self.log.Write("Begin: Job Execute")

            status = ""
            errorCd = ""
            errorMsg = ""

            logdbcon=self.logdb.GetLogDBConnection()
            logdbcur=logdbcon.cursor()

            if self.p.getParameter('etl_db.type').lower() == "oracle":
                sql = ""
                sql += " SELECT "
                sql += " js.job_step_cd,js.job_program_cd,js.job_program_action"
                sql += ",js.job_program_arg1,js.job_program_arg2,js.job_program_arg3"
                sql += ",js.job_program_arg4,js.job_program_arg5,js.job_program_arg6"
                sql += ",js.job_program_arg7,js.job_program_arg8,js.job_program_arg9"
                sql += ",js.job_program_arg10"
                sql += ",jw.last_run_start_dt,j.last_success_run_dt"
                sql += ",j.job_group_cd,j.job_cd"
                sql += ",j.loc_cd,j.org_cd,j.grp_cd"
                sql += " FROM etl_job_step js"
                sql += " INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                sql += " INNER JOIN etl_job_worker jw ON(jw.job_worker_cd = j.job_worker_cd AND jw.grp_cd = j.grp_cd AND jw.record_status = 'A')"
                sql += " WHERE js.job_step_cd = :job_step_cd "
                sql += " AND j.job_cd = :job_cd "
                sql += " AND jw.job_worker_cd = :job_worker_cd "
                sql += " AND js.grp_cd = :grp_cd "
                sql += " AND j.grp_cd = :grp_cd "
                sql += " AND jw.grp_cd = :grp_cd "
                sql += " AND js.record_status = 'A'"
            elif self.p.getParameter('etl_db.type').lower() == "postgresql":
                sql = ""
                sql += " SELECT "
                sql += " js.job_step_cd,js.job_program_cd,js.job_program_action"
                sql += ",js.job_program_arg1,js.job_program_arg2,js.job_program_arg3"
                sql += ",js.job_program_arg4,js.job_program_arg5,js.job_program_arg6"
                sql += ",js.job_program_arg7,js.job_program_arg8,js.job_program_arg9"
                sql += ",js.job_program_arg10"
                sql += ",jw.last_run_start_dt,j.last_success_run_dt"
                sql += ",j.job_group_cd,j.job_cd"
                sql += ",j.loc_cd,j.org_cd,j.grp_cd"
                sql += " FROM etl_job_step js"
                sql += " INNER JOIN etl_job j ON(j.job_cd = js.job_cd AND j.grp_cd = js.grp_cd AND j.record_status = 'A')"
                sql += " INNER JOIN etl_job_worker jw ON(jw.job_worker_cd = j.job_worker_cd AND jw.grp_cd = j.grp_cd AND jw.record_status = 'A')"
                sql += " WHERE js.job_step_cd = %(job_step_cd)s "
                sql += " AND j.job_cd = %(job_cd)s"
                sql += " AND jw.job_worker_cd = %(job_worker_cd)s"
                sql += " AND js.grp_cd = %(grp_cd)s"
                sql += " AND j.grp_cd = %(grp_cd)s"
                sql += " AND jw.grp_cd = %(grp_cd)s"
                sql += " AND js.record_status = 'A'"

            ldic = {}
            ldic['job_cd'] = p_job_cd
            ldic['job_step_cd'] = p_job_step_cd
            ldic['job_worker_cd'] = p_job_worker_cd
            ldic['grp_cd'] = p_job_grp_cd
            self.log.Write(sql, ldic)
            logdbcur.execute(sql, ldic)
            self.log.Write("rowcount=", logdbcur.rowcount)
            lrowjs = logdbcur.fetchone()
            logdbcur.close()

            self.log.Write(lrowjs)
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10,lv_job_worker_last_run_start_dt,lv_job_last_success_run_dt,lv_job_group_cd,lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = lrowjs

            if (lv_job_program_action == "SYNC_TAB2TAB"):
                # CALL Table Sync Method
                self.log.Write("BEGIN: Table2Table CALL")
                lvTable2Table = Table2Table(self.log)
                jobStepStatus, errorCd, errorMsg = lvTable2Table.SyncDataFromTableToTable(lrowjs, self.wdb, self.p)
                self.log.Write("OUTPUT:")
                self.log.Write(jobStepStatus, errorCd, errorMsg)

                self.log.Write("END: Table2Table CALL")

            elif (lv_job_program_action == "EXEC_PLSQL"):

                if (lv_job_group_cd == "JG_KIMS_DCDB2CDB_SAP"):
                    ldic = {}
                    ldic["PFROMTIME"] = Date.UTC2("IST", lv_job_last_success_run_dt)
                    ldic["PTOTIME"] = Date.UTC2("IST", lv_job_worker_last_run_start_dt)
                    ldic["PCOSTCENTERCD"] = lv_job_program_arg10
                    ldic["PPLANT_CODE"] = lv_job_program_arg5

                    self.wdb.GetDestinationDBConnection()
                    ddbcur=self.wdb.dcon.cursor()
                    sql=""
                    sql+=" begin"
                    sql+=" "+lv_job_program_arg2+";"
                    sql+=" end;"

                    self.log.Write(sql, ldic)
                    ddbcur.execute(sql, ldic)
                    self.log.Write("rowcount=", ddbcur.rowcount)
                    ddbcur.close()
                    self.wdb.dcon.commit()

            elif (lv_job_program_action == "SYNC_TAB2IDX"):

                # CALL Index Sync Method
                self.log.Write("BEGIN: Table2Index CALL")

                lvTable2Index= Table2Index(self.log)
                jobStepStatus, errorCd, errorMsg = lvTable2Index.SyncDataFromTableToIndex(lrowjs, self.logdb,self.wdb, self.p)
                self.log.Write("OUTPUT:")
                self.log.Write(jobStepStatus, errorCd, errorMsg)

                self.log.Write("END: Table2Index CALL")

            elif (lv_job_program_action == "COPY_TAB2FILE"):

                self.log.Write("BEGIN: Table2File CALL")

                lvTable2File = Table2File(self.log)
                jobStepStatus, errorCd, errorMsg = lvTable2File.CopyDataFromTableToXMLFile(lrowjs, self.wdb, self.p)
                self.log.Write("OUTPUT:")
                self.log.Write(jobStepStatus, errorCd, errorMsg)

                self.log.Write("END: Table2File CALL")
            elif (lv_job_program_action == "COPY_FILE2TAB"):

                self.log.Write("BEGIN: File2Table CALL")

                obj = File2Table(self.log)
                jobStepStatus, errorCd, errorMsg = obj.CopyDataFromXMLFileToTable(lrowjs, self.wdb, self.p)
                self.log.Write("OUTPUT:")
                self.log.Write(jobStepStatus, errorCd, errorMsg)

                self.log.Write("END: File2Table CALL")
            else:
                None

            status = 0
            errorCd = ""
            errorMsg = ""
            self.log.Write("End: Job Execute : NoException")

        except BaseException as e:
            self.log.Write(traceback.print_exc())

            if logdbcur:
                logdbcur.close()

            status = 1
            errorCd = str(e)
            errorMsg = str(e)

            self.log.Write("End: Job Execute : Exception")

        finally:
            None
            return status,errorCd,errorMsg