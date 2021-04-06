Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

$taskName = $args[0]
$taskPath = $args[1]

Enable-ScheduledTask -TaskName $taskName -TaskPath $taskPath

