@echo off
:: ─────────────────────────────────────────────────────────
::  MapReduce PJBL1 — Big Data PUCPR
::  Compila (se necessário) e executa o menu interativo.
:: ─────────────────────────────────────────────────────────

cd /d "%~dp0"

set JAR=target\mapreduce-pjbl1.jar

if not exist "%JAR%" (
    echo Compilando projeto com Maven...
    call mvn -q package -DskipTests
    if errorlevel 1 (
        echo Erro na compilacao. Verifique se Maven e Java estao instalados.
        pause
        exit /b 1
    )
    echo Compilacao concluida.
)

echo Iniciando aplicacao...
java -jar "%JAR%" %*
