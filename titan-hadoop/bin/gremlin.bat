@echo off

SET STARTDIR=%CD%
cd %CD%\..\target\

set TARGET=

for /f "tokens=*" %%a in ('dir /b /ad') do (
if exist "%%a\bin\gremlin.bat" set TARGET=%%a
)

cd %TARGET%\bin\
call gremlin.bat %*

cd %STARTDIR%