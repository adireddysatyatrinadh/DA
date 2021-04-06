Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

$taskName = $args[0]

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
