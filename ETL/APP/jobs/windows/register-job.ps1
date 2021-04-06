

Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted


#$scheduleObject = New-Object -ComObject schedule.service
#$scheduleObject.connect()
#$rootFolder = $scheduleObject.GetFolder("\")
#$rootFolder.DeleteFolder("poshTasks",$unll)

#$scheduleObject = New-Object -ComObject schedule.service
#$scheduleObject.connect()
#$rootFolder = $scheduleObject.GetFolder("\")
#$rootFolder.CreateFolder("ETL")



#Unregister-ScheduledTask -TaskName "Task1" -TaskPath "\ETL" -Confirm:$false

# register-job.ps1 "Task1" "\ETL" "Task1Desription" "A.EXEC" “A_KIMS_FILE2FILE_DA" "G:/ETL/APP/bin" "01/01/2021 00:10:00 AM"

$taskName = $args[0]
$taskPath = $args[1]
$taskDescription = $args[2]
$command = $args[3]
$argument = $args[4]
$workingDir = $args[5]
$startTime=$args[6]
$repeatTime=$args[7]



#$taskName = "Task1"
#$taskPath = "\DA"
#$taskDescription = "Task1Description"
#$argument = “A_KIMS_FILE2FILE_DA"
#$workingDir = "G:/ETL/APP/bin"
#$command = "A_EXEC.bat"
#$startTime="01/01/2021 00:10:00 AM"

#Unregister-ScheduledTask -TaskName $taskName -Confirm:$false

#Credentials to run task as
$username = "CENTRALDB.kimshq.local" #current user
$password = "K1m5@SCHkims123"
echo $username, $password

$repeat = (New-TimeSpan -Minutes $repeatTime)
$duration = (New-TimeSpan -Days 3650)
$everyMinute = (New-TimeSpan -Minutes 1)

$trigger = New-ScheduledTaskTrigger -At $startTime -Once -RepetitionInterval $repeat -RepetitionDuration $duration 
$action=New-ScheduledTaskAction -Execute $command -WorkingDirectory $workingDir -Argument $argument
$settings = New-ScheduledTaskSettingsSet -Hidden -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable -ExecutionTimeLimit $duration -RestartInterval $everyMinute -RestartCount 999 -Priority 0 -DisallowHardTerminate
$pricipal=New-ScheduledTaskPrincipal -LogonType S4U -UserId $username

Register-ScheduledTask -TaskName $taskName -TaskPath $taskPath -Description $taskDescription -Action $action -Trigger $trigger -Settings $settings # -Principal $pricipal
#-User $username -Password $password

$task = Get-ScheduledTask -TaskName $taskName
$task.Triggers[0].ExecutionTimeLimit = "P1D"
#$task.Principal.LogonType ="Password"
Set-ScheduledTask $task
