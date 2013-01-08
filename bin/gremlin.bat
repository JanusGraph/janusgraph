:: Windows launcher script for Gremlin
@echo off

cd %CD%\..titan-all\target\

set TARGET=

for /f "tokens=*" %%a in ('dir /b /ad') do (
if exist "%%a\bin\gremlin.bat" set TARGET=%%a
)

cd %TARGET%\bin\
call gremlin.bat %*