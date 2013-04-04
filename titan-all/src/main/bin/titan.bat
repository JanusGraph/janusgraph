:: Windows launcher script for Titan

@echo off

set LIBDIR=..\lib

set OLD_CLASSPATH=%CLASSPATH%
set CP=

set JAVA_OPTIONS=-Xms1G^
 -Xmx1G^
 -Dcom.sun.management.jmxremote.port=7199^
 -Dcom.sun.management.jmxremote.ssl=false^
 -Dcom.sun.management.jmxremote.authenticate=false

echo %JAVA_OPTIONS%

:: Launch the application

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %LIBDIR%/*; com.thinkaurelius.titan.tinkerpop.rexster.RexsterTitanServer %*