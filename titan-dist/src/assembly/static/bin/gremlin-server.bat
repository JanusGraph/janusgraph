:: Windows launcher script for Gremlin Server
@echo off

set work=%CD%

if [%work:~-3%]==[bin] cd ..

set LIBDIR=lib

set JAVA_OPTIONS=-Xms32m -Xmx512m -javaagent:%LIBDIR%/jamm-0.3.0.jar

:: Launch the application
java -Dlog4j.configuration=../conf/log4j-server.properties %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*; org.apache.tinkerpop.gremlin.server.GremlinServer %*
