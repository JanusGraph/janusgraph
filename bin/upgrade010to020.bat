:: Windows launcher script for Titan
@echo off

cd %~dp0
cd ..\titan-dist\titan-dist-all\target\titan-all-standalone\bin
call upgrade010to020.bat %*
