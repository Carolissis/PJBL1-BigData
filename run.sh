#!/bin/bash
# ─────────────────────────────────────────────────────────
#  MapReduce PJBL1 — Big Data PUCPR
#  Compila (se necessário) e executa o menu interativo.
# ─────────────────────────────────────────────────────────

set -e
cd "$(dirname "$0")"

JAR="target/mapreduce-pjbl1.jar"

if [ ! -f "$JAR" ]; then
    echo ">>> Compilando projeto com Maven..."
    mvn -q package -DskipTests
    echo ">>> Compilação concluída."
fi

echo ">>> Iniciando aplicação..."
java -jar "$JAR" "$@"
