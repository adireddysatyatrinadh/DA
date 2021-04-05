import sys
import datetime
from Date import Date
import traceback

class PLpgSQL:

    def __init__(self,pLog):
        self.log=pLog

    def ExecuteFunction(self, prowjs,pwdb,pParameter):

        self.Parameter = pParameter

        try:
            lv_job_step_cd, lv_job_program_cd, lv_job_program_action, lv_job_program_arg1, lv_job_program_arg2, iv_job_program_arg3, lv_job_program_arg4, lv_job_program_arg5, lv_job_program_arg6, lv_job_program_arg7, lv_job_program_arg8, lv_job_program_arg9, lv_job_program_arg10, lv_job_worker_last_run_start_dt, lv_job_last_success_run_dt, lv_job_group_cd, lv_job_cd,lv_job_loc_cd,lv_job_org_cd,lv_job_grp_cd = prowjs

            print(lv_job_cd,lv_job_step_cd)
            if (lv_job_group_cd == "JG_KIMS_PGSQL-DA2DL_DA"):
                llis = list()
                llis.append(lv_job_program_arg10)
                llis.append(lv_job_last_success_run_dt)
                llis.append(lv_job_worker_last_run_start_dt)
                print(llis)
                cur = pwdb.scon.cursor()
                sql = " select * from  " + lv_job_program_arg2 + "(%s,%s,%s)" 
                self.log.Write(sql, llis)
                cur.execute(sql, tuple(llis))
                status, errorCd, errorMsg=cur.fetchone()
                self.log.Write("rowcount=", cur.rowcount)
                cur.close()
                pwdb.scon.commit()
            else:
                None

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
    #END: ExecuteFunction()
