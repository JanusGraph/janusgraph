:: Windows launcher script for Titan

@echo off

set LIBDIR=..\lib
set EXTDIR=..\ext

set OLD_CLASSPATH=%CLASSPATH%
set CP=

set JAVA_OPTIONS=-Xms32m -Xmx512m

:: Launch the application

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*;%EXTDIR%/*; com.thinkaurelius.titan.upgrade.Upgrade010to020 %*