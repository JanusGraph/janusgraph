:: Windows launcher script for Rexster Console
@echo off

set work=%CD%

if [%work:~-3%]==[bin] cd ..

set LIBDIR=lib

set JAVA_OPTIONS=-Xms32m -Xmx512m

:: Launch the application
java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*; com.tinkerpop.rexster.console.RexsterConsole %*