:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::   http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.

:: Windows launcher script for Gremlin Console

@ECHO OFF
SETLOCAL EnableDelayedExpansion
SET work=%CD%

IF [%work:~-3%]==[bin] CD ..

IF NOT DEFINED JANUSGRAPH_HOME (
    SET JANUSGRAPH_HOME=%CD%
)

:: location of the JanusGraph lib directory
SET JANUSGRAPH_LIB=%JANUSGRAPH_HOME%\lib

:: location of the JanusGraph extensions directory
IF NOT DEFINED JANUSGRAPH_EXT (
    SET JANUSGRAPH_EXT=%JANUSGRAPH_HOME%\ext
)

:: Set default message threshold for Log4j Gremlin's console appender
IF NOT DEFINED GREMLIN_LOG_LEVEL (
    SET GREMLIN_LOG_LEVEL=WARN
)

:: Hadoop winutils.exe needs to be available because hadoop-gremlin is installed and active by default
IF NOT DEFINED HADOOP_HOME (
    SET JANUSGRAPH_WINUTILS=%JANUSGRAPH_HOME%\bin\winutils.exe
    IF EXIST !JANUSGRAPH_WINUTILS! (
        SET HADOOP_HOME=%JANUSGRAPH_HOME%
    ) ELSE (
        ECHO HADOOP_HOME is not set.
        ECHO Download https://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
        ECHO Place it under !JANUSGRAPH_WINUTILS!
        PAUSE
        GOTO :eof
    )
)

:: set HADOOP_GREMLIN_LIBS by default to the JanusGraph lib
IF NOT DEFINED HADOOP_GREMLIN_LIBS (
    SET HADOOP_GREMLIN_LIBS=%JANUSGRAPH_LIB%
)

CD %JANUSGRAPH_LIB%

FOR /F "tokens=*" %%G IN ('dir /b "janusgraph-*.jar"') DO SET JANUSGRAPH_JARS=!JANUSGRAPH_JARS!;%JANUSGRAPH_LIB%\%%G

FOR /F "tokens=*" %%G IN ('dir /b "jamm-*.jar"') DO SET JAMM_JAR=%JANUSGRAPH_LIB%\%%G

FOR /F "tokens=*" %%G IN ('dir /b "slf4j-log4j12-*.jar"') DO SET SLF4J_LOG4J_JAR=%JANUSGRAPH_LIB%\%%G

CD %JANUSGRAPH_EXT%

FOR /D /r %%i in (*) do (
    SET EXTDIR_JARS=!EXTDIR_JARS!;%%i\*
)

CD %JANUSGRAPH_HOME%

:: put slf4j-log4j12 and JanusGraph jars first because of conflict with logback
SET CP=%CLASSPATH%;%SLF4J_LOG4J_JAR%;%JANUSGRAPH_JARS%;%JANUSGRAPH_LIB%\*;%EXTDIR_JARS%

:: jline.terminal workaround for https://issues.apache.org/jira/browse/GROOVY-6453
:: to debug plugin :install include -Divy.message.logger.level=4 -Dgroovy.grape.report.downloads=true
:: to debug log4j include -Dlog4j.debug=true
IF NOT DEFINED JAVA_OPTIONS (
 SET JAVA_OPTIONS=-Xms32m -Xmx512m ^
 -Dtinkerpop.ext=%JANUSGRAPH_EXT% ^
 -Dlogback.configurationFile=%JANUSGRAPH_HOME%\conf\logback.xml ^
 -Dlog4j.configuration=file:/%JANUSGRAPH_HOME%\conf\log4j-console.properties ^
 -Dgremlin.log4j.level=%GREMLIN_LOG_LEVEL% ^
 -Djline.terminal=none ^
 -javaagent:%JAMM_JAR% ^
 -Dgremlin.io.kryoShimService=org.janusgraph.hadoop.serialize.JanusGraphKryoShimService
)

:: Launch the application

IF "%1" == "" GOTO console
IF "%1" == "-e" GOTO script
IF "%1" == "-v" GOTO version

:: Start the Gremlin Console

:console

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% org.apache.tinkerpop.gremlin.console.Console %*

GOTO finally

:: Evaluate a Groovy script file

:script

SET strg=

FOR %%X IN (%*) DO (
CALL :concat %%X %1 %2
)

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% org.apache.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor %strg%

GOTO finally

:: Print the version

:version

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% org.janusgraph.core.JanusGraph

GOTO finally


:concat

IF %1 == %2 GOTO finally

SET strg=%strg% %1


:finally

ENDLOCAL
