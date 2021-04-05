import sys
import datetime
from Date import Date
import traceback

class PLSQL:

    def __init__(self,pLog):
        self.log=pLog

    def ExecuteProcedure(self, prowjs,pwdb,pParameter):

        self.Parameter = pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10, lv_job_worker_last_run_start_dt, lv_job_last_success_run_dt, lv_job_group_cd, lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs

            if (lv_job_group_cd in ['JG_KIMS_DCDB2CDB_SAP','JG_KIMS_DCDB2CDB_SAP_FIN']):
                ldic = {}
                ldic["PFROMTIME"] = lv_job_last_success_run_dt
                ldic["PTOTIME"] = lv_job_worker_last_run_start_dt
                ldic["PCOSTCENTERCD"] = lv_job_program_arg10
                ldic["PPLANT_CODE"] = lv_job_program_arg5

                cur = pwdb.dcon.cursor()
                sql = ""
                sql += " begin"
                sql += " " + lv_job_program_arg2 + ";"
                sql += " end;"

                self.log.Write(sql, ldic)
                cur.execute(sql, ldic)
                self.log.Write("rowcount=", cur.rowcount)
                cur.close()
                pwdb.dcon.commit()
            elif (lv_job_group_cd == "JG_KIMS_HIMS2DA_DA"):
                ldic = {}
                ldic["PFROM_DT"] = lv_job_last_success_run_dt
                ldic["PTO_DT"] = lv_job_worker_last_run_start_dt
                ldic["PJOB_CD"] = lv_job_program_arg4
                ldic["PDW_FACILITY_CD"] = lv_job_program_arg5

                cur = pwdb.dcon.cursor()
                sql = ""
                sql += " begin"
                sql += " " + lv_job_program_arg7 + "." + lv_job_program_arg2 + ";"
                sql += " end;"

                self.log.Write(sql, ldic)
                cur.execute(sql, ldic)
                self.log.Write("rowcount=", cur.rowcount)
                cur.close()
                pwdb.dcon.commit()
            else:
                None

            status=0
            errorCd=""
            errorMsg=""

            return status,errorCd,errorMsg

        except BaseException as e:
            status=1
            errorCd=str(e)
            errorMsg=str(e)
            #print(str(traceback.print_exc()))
            self.log.Write(traceback.print_exc())

            if cur:
                cur.close()

        finally:
            return status, errorCd, errorMsg
    #END: ExecuteProcedure()