:: Windows launcher script for Titan

@echo off

set work=%CD%

if [%work:~-3%]==[bin] cd ..

set LIBDIR=lib
set EXTDIR=ext/*

cd ext

FOR /D /r %%i in (*) do (
    set EXTDIR=%EXTDIR%;%%i/*
)

cd ..

set OLD_CLASSPATH=%CLASSPATH%
set CP=

set JAVA_OPTIONS=-Xms1G^
 -Xmx1G^
 -Dcom.sun.management.jmxremote.port=7199^
 -Dcom.sun.management.jmxremote.ssl=false^
 -Dcom.sun.management.jmxremote.authenticate=false

echo %JAVA_OPTIONS%

:: Launch the application

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%; com.thinkaurelius.titan.tinkerpop.rexster.RexsterTitanServer %*
