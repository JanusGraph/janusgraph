:: Windows launcher script for Titan
@echo off

cd %CD%\..titan-all\target\

set TARGET=

for /f "tokens=*" %%a in ('dir /b /ad') do (
if exist "%%a\bin\titan.bat" set TARGET=%%a
)

cd %TARGET%\bin\
call titan.bat %*