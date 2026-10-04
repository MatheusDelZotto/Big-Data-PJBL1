# PjBL 1 — MapReduce (Hadoop / Java)

Soluções MapReduce para as 9 questões sobre o dataset `operacoes_comerciais_inteira.csv`
(8.222.825 transações, 10 colunas separadas por `;`).

## Estrutura

O código segue o padrão dos exemplos da aula (WordCount, AverageTemperature, AverageCombinerWind,
EntropyFASTA): cada questão é uma classe com `main` + classes internas `Map…`/`Combine…`/`Reduce…`,
`BasicConfigurator.configure()`, `new Job(c, "...")`, caminhos fixos `in/…` e `output/…`, e os
writables customizados são POJOs com getters/setters + `write`/`readFields`.

```
src/main/java/pjbl/
├── writables/
│   ├── MediaWritable.java            # (soma, qtd)                         -> Q5, Q7
│   ├── MinMaxWritable.java           # menor e maior transação (valor, mercadoria, fluxo) -> Q6, Q9
│   ├── AnoPaisWritable.java          # WritableComparable (ano, país)      -> chave da Q9
│   └── ValorDecrescenteWritable.java # WritableComparable com compareTo invertido -> ordenação da Q8
├── q1/Q1TransacoesBrasil.java
├── q2/Q2TransacoesPorAno.java
├── q3/Q3TransacoesPorCategoria.java
├── q4/Q4TransacoesPorFluxo.java
├── q5/Q5MediaPorAnoBrasil.java
├── q6/Q6MaisCaraMaisBarataBrasil2016.java
├── q7/Q7MediaExportacaoPorAnoBrasil.java
├── q8/Q8MaximoPorAnoBrasilOrdenado.java
└── q9/Q9MaiorMenorQuantidadePorAnoPais.java
resultados/q1.txt … q9.txt   # resultado de cada questão
```

## Como executar

Coloque o CSV em `in/operacoes_comerciais_inteira.csv` (caminho fixo no código, como nos exemplos da aula).

- **IntelliJ**: rodar o `main` da classe da questão. A saída vai para `output/qN`
  (apague a pasta antes de rodar de novo; o Hadoop não sobrescreve a saída).
- **Terminal** (Java 8+ e Maven): `./executar.sh` roda todas, `./executar.sh 5 9` roda só algumas.
  O script apaga `output/qN`, executa o job e copia `output/qN/part-r-*` para `resultados/qN.txt`.

## Requisitos do enunciado

| Requisito | Como foi atendido |
|---|---|
| Retirar o cabeçalho | Em todo mapper: `if (linha.startsWith("country_or_area")) { return; }` (mesmo padrão do `>gi` no EntropyFASTA). |
| Dados faltantes | `col.length < 10` descarta linha incompleta; cada mapper testa `isEmpty()` nas colunas que usa. No dataset faltam `weight_kg` (128.445) e `quantity` (304.827) — a Q9 ignora essas 304.827 linhas. |
| Sem concatenação de strings em chaves/valores | Chaves/valores compostos são writables próprios. Strings só são montadas no `toString()`, para formatar a saída. |
| Q5 — writable customizado | `MediaWritable(soma, qtd)`, igual à ideia do `FireAvgTempWritable(temp, freq)` da aula. |
| Q6 — Combiner + writable | `MinMaxWritable`; classes `CombineForMinMax` e `ReduceForMinMax`. |
| Q7 — Combiner + writable | `CombineForMediaExportacao` soma (soma, qtd) parciais; a média só é calculada no reduce (média de médias daria errado). |
| Q8 — concatenação de jobs | Como no EntropyFASTA: job 1 grava `ano<TAB>máximo` em `output/q8_intermediario`; job 2 lê esse texto com `split("\t")` e usa a chave `ValorDecrescenteWritable`, cujo `compareTo` invertido faz o sort/shuffle ordenar do maior para o menor. |
| Q9 — Comparable writable + Combiner | Chave `AnoPaisWritable` (WritableComparable com `compareTo`) + `CombineForAnoPais`. Como nos exemplos da aula, o job usa 1 reducer (padrão do Hadoop), por isso não é preciso `hashCode`/`equals`; o agrupamento das chaves no reduce é feito pelo `compareTo`. |

## Decisões de interpretação

- **"Brasil"** = `country_or_area == "Brazil"` (único nome do país no dataset).
- **Valor/preço da transação** (Q5–Q8) = coluna `trade_usd`.
- **Q8**: o enunciado diz "crescente, do maior valor para o menor". Segui a parte explícita, **do maior para o menor** (decrescente).
- **Q9**: o enunciado diz "maior e menor preço (com base na coluna amount)", então usei a coluna `quantity` (amount), como pedido. Para usar `trade_usd`, basta trocar `col[8]` por `col[5]` no mapper.
- **Empates** em min/max (comum na Q9, onde muitas quantidades são 0) ficam com a mercadoria que vem primeiro em ordem alfabética, para o resultado não depender da ordem em que o Combiner processa os dados.
- O dataset tem linhas agregadas `TOTAL - ALL COMMODITIES` (categoria `all_commodities`). Elas foram mantidas como transações comuns e por isso aparecem como "mais cara" na Q6 e nos máximos da Q8. Se o professor quiser excluí-las, é só descartar `col[9].equals("all_commodities")` nos mappers.

## Formato dos resultados

- `q1.txt`: `Brazil <tab> contagem`
- `q2/q3/q4.txt`: `chave <tab> contagem`
- `q5/q7.txt`: `ano <tab> média (USD)` — saída `DoubleWritable`, como nos exemplos (ex.: `1.6514942883511633E7` = 16.514.942,88)
- `q6.txt`: `Brazil 2016 <tab> MENOR: valor (mercadoria - fluxo) <tab> MAIOR: valor (mercadoria - fluxo)`
- `q9.txt`: `ano <tab> país <tab> MENOR: valor (mercadoria - fluxo) <tab> MAIOR: valor (mercadoria - fluxo)`
- Em Q6/Q9 o valor é `double` impresso pelo Java, então valores grandes aparecem em notação científica (ex.: `1.85235399101E11` = 185.235.399.101).
- `q8.txt`: `ano <tab> valor máximo`, do maior valor para o menor
