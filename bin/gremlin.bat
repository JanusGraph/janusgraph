:: Windows launcher script for Gremlin
@echo off

cd %~dp0
cd ..\titan-dist\titan-dist-all\target\titan-all-standalone\bin
call gremlin.bat %*
