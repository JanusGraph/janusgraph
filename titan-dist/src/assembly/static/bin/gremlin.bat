:: Windows launcher script for Gremlin Groovy
@echo off

set LIBDIR=%~dp0\..\lib
set EXTDIR=%~dp0\..\ext

set OLD_CLASSPATH=%CLASSPATH%
set CP=

setLocal EnableDelayedExpansion
set CP=%LIBDIR%\*
for /D /R %EXTDIR% %%d in (*) do (set CP=!CP!;%%d\*.jar)

:: Launch the application

if "%1" == "" goto console

if "%1" == "-e" goto script

if "%1" == "-v" goto version


:console

set CLASSPATH=!CP!;%OLD_CLASSPATH%
java %JAVA_OPTIONS% %JAVA_ARGS% com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.Console %*
set CLASSPATH=%OLD_CLASSPATH%
goto :eof


:script

set strg=

FOR %%X IN (%*) DO (
CALL :concat %%X %1 %2
)

set CLASSPATH=!CP!;%OLD_CLASSPATH%
java %JAVA_OPTIONS% %JAVA_ARGS% com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.ScriptExecutor %strg%
set CLASSPATH=%OLD_CLASSPATH%
goto :eof


:version

set CLASSPATH=!CP!;%OLD_CLASSPATH%
java %JAVA_OPTIONS% %JAVA_ARGS% com.tinkerpop.gremlin.Version
set CLASSPATH=%OLD_CLASSPATH%
goto :eof


:concat

if %1 == %2 goto skip

SET strg=%strg% %1


:skip
