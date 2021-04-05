import datetime
import traceback

class Date:

    def __init__(self,pLog):
        self.log=pLog
        self.log.Write("in constructor")

    @staticmethod
    def GetUTCDateTime(self):
        cdt = datetime.datetime.utcnow()
        cdtstr = cdt.strftime('%Y:%m:%d %H:%M:%S.%f')
        return cdtstr

    def DateDiff(self,fromdt,todt):
        try:
            duration = todt - fromdt
            #self.log.Write(duration)
            #self.log.Write(duration.days)
            #self.log.Write(duration.seconds)
            #self.log.Write(duration.microseconds)
            #self.log.Write(duration.total_seconds())

            hours = divmod(duration.total_seconds(), 3600)
            minutes = divmod(hours[1], 60)
            seconds = divmod(minutes[1], 1)
            self.log.Write(hours,minutes,seconds)

            hours=hours[0]
            minutes=minutes[0]
            seconds=seconds[0]
            #milliseconds = seconds[1]

            duration=str(int(hours))+":"+str(int(minutes))+":"+str(int(seconds))
            self.log.Write(duration)
            return duration
        except BaseException as e:
            self.log.Write(traceback.print_exc())
            return None

    def NextRunDate(self,p_current_sch_dt,p_sch_cd):
        try:
            lv_next_sch_dt=p_current_sch_dt
            while True:
                if (p_sch_cd=="S_MINUTE_ONCE"):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(minutes=1)
                elif (p_sch_cd=="S_HOURLY_ONCE" or p_sch_cd is None ):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(hours=1)
                elif (p_sch_cd=="S_DAILY_ONCE"):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(days=1)
                elif (p_sch_cd=="S_WEEKLY_ONCE"):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(weeks=1)
                elif (p_sch_cd=="S_MONTHLY_ONCE"):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(days=30)
                elif (p_sch_cd=="S_YEARLY_ONCE"):
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(days=365)
                else:
                    lv_next_sch_dt=lv_next_sch_dt+datetime.timedelta(minutes=1)

                if(lv_next_sch_dt > datetime.datetime.now()):
                    break
            return lv_next_sch_dt
        except BaseException as e:
               self.log.Write(traceback.print_exc())

    @staticmethod
    def TZ2UTC(p_From_TimeZone, p_dt):
        try:
            if (p_From_TimeZone=="IST"):
                lv_tz_dt=p_dt+datetime.timedelta(minutes=-330)
            else:
                lv_tz_dt=p_dt

            return lv_tz_dt
        except BaseException as e:
               self.log.Write(traceback.print_exc())
    @staticmethod
    def UTC2TZ(p_To_TimeZone, p_dt):
        try:
            if (p_To_TimeZone=="IST"):
                lv_tz_dt=p_dt+datetime.timedelta(minutes=330)
            else:
                lv_tz_dt=p_dt

            return lv_tz_dt
        except BaseException as e:
               self.log.Write(traceback.print_exc())