:: Windows launcher script for Titan
@echo off

cd %CD%\..\target\

set TARGET=

for /f "tokens=*" %%a in ('dir /b /ad') do (
if exist "%%a\bin\upgrade010to020.bat" set TARGET=%%a
)

cd %TARGET%\bin\
call upgrade010to020.bat %*