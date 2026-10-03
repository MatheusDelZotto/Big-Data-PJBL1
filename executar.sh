#!/usr/bin/env bash
# Executa os jobs em modo local e grava cada resultado em resultados/qN.txt.
# Os caminhos ficam fixos no código (como nos exemplos da aula):
#   entrada: in/operacoes_comerciais_inteira.csv   saída: output/qN
# Uso: ./executar.sh            (todas as questões)
#      ./executar.sh 5 9        (só algumas)
set -euo pipefail
cd "$(dirname "$0")"

QUESTOES=("$@")
[ ${#QUESTOES[@]} -eq 0 ] && QUESTOES=(1 2 3 4 5 6 7 8 9)

declare -A CLASSES=(
  [1]=pjbl.q1.Q1TransacoesBrasil
  [2]=pjbl.q2.Q2TransacoesPorAno
  [3]=pjbl.q3.Q3TransacoesPorCategoria
  [4]=pjbl.q4.Q4TransacoesPorFluxo
  [5]=pjbl.q5.Q5MediaPorAnoBrasil
  [6]=pjbl.q6.Q6MaisCaraMaisBarataBrasil2016
  [7]=pjbl.q7.Q7MediaExportacaoPorAnoBrasil
  [8]=pjbl.q8.Q8MaximoPorAnoBrasilOrdenado
  [9]=pjbl.q9.Q9MaiorMenorQuantidadePorAnoPais
)

MVN="$(command -v mvn || echo "$HOME/.sdkman/candidates/maven/current/bin/mvn")"
"$MVN" -q -B package
[ -f target/cp.txt ] || "$MVN" -q -B dependency:build-classpath -Dmdep.outputFile=target/cp.txt
CP="target/pjbl1-mapreduce-1.0.jar:$(cat target/cp.txt)"

mkdir -p resultados output
for q in "${QUESTOES[@]}"; do
  echo ">>> Questão $q (${CLASSES[$q]})"
  rm -rf "output/q$q" "output/q${q}_intermediario"   # o Hadoop não sobrescreve a pasta de saída
  java -cp "$CP" "${CLASSES[$q]}" -D mapreduce.local.map.tasks.maximum="$(nproc)" > "output/q$q.log" 2>&1
  cat output/q$q/part-r-* > "resultados/q$q.txt"
  echo "    -> resultados/q$q.txt ($(wc -l < resultados/q$q.txt) linhas)"
done
