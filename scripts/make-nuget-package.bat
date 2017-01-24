@echo off

REM
REM Build and package kafka-sharp as a NuGet package.
REM
REM You will need to have nuget.exe in your path. If you don't already have it,
REM download it from https://dist.nuget.org/index.html (Windows x86 Commandline).
REM

REM The project.json file from the .NET Core Visual Studio project interfers with the nuget packaging step.
REM Get it out of the way temporarily by renaming it.
ren ..\kafka-sharp\kafka-sharp\project.json project.json.RENAMED

REM Build and package kafka-sharp.
nuget.exe pack ..\kafka-sharp\kafka-sharp\Kafka.csproj -Verbosity detailed -Build -Prop Configuration=Release -IncludeReferencedProjects -OutputDirectory ..\nuget

REM Revert the renaming step performed earlier.
ren ..\kafka-sharp\kafka-sharp\project.json.RENAMED project.json

pause
