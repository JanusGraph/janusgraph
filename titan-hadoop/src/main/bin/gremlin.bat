@ECHO OFF
setlocal enabledelayedexpansion

SET LIBDIR=..\lib
SET CP=%LIBDIR%/*

:: find hadoop
IF "%HADOOP_PREFIX%" NEQ "" (
  SET CP=%CP%;%HADOOP_PREFIX%/conf
) ELSE (
  IF "%HADOOP_CONF_DIR%" NEQ "" (
    SET CP=%CP%;%HADOOP_CONF_DIR%
  ) ELSE (
    IF "%HADOOP_CONF%" NEQ "" (
      SET CP=%CP%;%HADOOP_CONF%
    ) ELSE (
      SET CP=%CP%;%HADOOP_HOME%/conf
    )
  )
)

IF "%JAVA_OPTIONS%" EQU "" (
  SET JAVA_OPTIONS=-Xms32m -Xmx512m
)

SET K=

IF "%1" EQU "-e" (
  FOR %%A IN (%*) DO (
    IF "%%A" NEQ "-e" (
      SET K=!K! %%A
    )
  )

  java %JAVA_OPTIONS% -cp %CP%;%CLASSPATH% com.thinkaurelius.faunus.tinkerpop.gremlin.ScriptExecutor !K!
  GOTO done
)

IF "%1" EQU "-i" (
  FOR %%A IN (%*) DO (
    IF "%%A" NEQ "-i" (
      SET K=!K! %%A
    )
  )

  java %JAVA_OPTIONS% -cp %CP%;%CLASSPATH% com.thinkaurelius.faunus.tinkerpop.gremlin.InlineScriptExecutor !K!
  GOTO done
)

IF "%1" EQU "-v" (
  java %JAVA_OPTIONS% -cp %CP%;%CLASSPATH% com.thinkaurelius.faunus.tinkerpop.gremlin.Version
) ELSE (
  java %JAVA_OPTIONS% -cp %CP%;%CLASSPATH% com.thinkaurelius.faunus.tinkerpop.gremlin.Console
)

GOTO done

:done