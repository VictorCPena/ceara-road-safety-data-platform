# Validação local Kafka → Spark → Delta

## Situação em 06/11/2026

Evidências fornecidas pelo usuário no Mac Intel: Colima iniciou o Docker 29.5.2;
Redpanda v24.2.9 respondeu a `rpk cluster info`; o tópico `prf-replay` foi criado;
o cliente Python consultou os metadados pela porta 19092 e encontrou uma partição.
Uma tentativa IPv6 foi recusada antes do sucesso. Esses comandos comprovam
conectividade/metadados, ainda não publicação/consumo pelo Spark.

Na implementação desta atualização: linha de base 5 testes aprovados; depois,
7 testes aprovados, 142 avisos de depreciação, 18,96 s. Ambiente Linux x86_64,
Python 3.12.14, Java 21.0.13, PySpark 3.5.8; a implantação recomendada continua
Python 3.12 e Java 17. Os testes usam Spark real, fonte de arquivos e Delta real.
Ruff aprovou os três arquivos Python novos/alterados. **Não há Docker disponível
neste ambiente. Posteriormente o usuário executou e aprovou o caminho Kafka no Mac,
conforme o resultado documentado abaixo.**

A primeira tentativa da linha de base local falhou em 5 testes na conversão de
timezone do Pandas/PySpark, com `AttributeError` em `tz_localize`; a repetição com
`TZ=UTC` passou os 5 testes. Os logs das duas tentativas foram preservados. Isso
não foi confundido com a execução Mac já aprovada pelo usuário.

## Executar no Mac

Depois de aplicar o pacote de atualização:

```bash
cd "$HOME/Downloads/portfolio-internacional-dados-ia/ceara-road-safety-lakehouse-pro"
source .venv312/bin/activate
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
export TZ=UTC
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"

python -m pytest tests/test_pro_lakehouse.py tests/test_stream_commit_recovery.py -q &&
python -m lakehouse.kafka_validation --csv data/raw/datatran2024.csv
```

O processo usa o Redpanda já iniciado. Na primeira execução, o Spark baixa do
Maven Central o conector oficial
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8` e dependências para seu cache
Ivy. Esse conector não vem apenas com `pip install pyspark`. É necessário acesso
de rede ao Maven; o código não instala serviços pagos nem ativa cloud.

## O que o comando verifica

1. Cria tópico exclusivo `prf-validation-<id>` com uma partição e uma réplica.
2. Normaliza o CSV real e escolhe até 20 ocorrências de Ceará recentes no arquivo,
   dentro da mesma janela de dois dias. A amostra é determinística para o CSV.
3. Publica os registros, uma duplicata e um envelope inválido deliberado.
4. Consome com Spark, grava Silver/Gold em Delta e exige exatamente uma rejeição
   na DLQ de arquivos. A Silver deve corresponder à amostra, sem duplicação.
5. Publica uma duplicata e um tombstone versão 2 **sintético**, usado apenas como
   teste. Injeta uma exceção após o commit Delta/Gold, antes do commit do checkpoint.
6. Retoma com o mesmo checkpoint; exige replay da entrada não confirmada, estado
   idempotente e reconciliação da Gold. Uma terceira inicialização vazia deve
   consumir zero entradas e preservar o resultado.

O stack trace contendo `Injected failure after Delta commit` é esperado durante
o cenário de falha. **Aprovação é somente o resultado final `"status": "passed"`
e a saída do processo com código zero.** Outro erro interrompe a execução e deixa
`result.json` com `status: failed`; nunca se deve contar uma execução parcial como
aprovada. Tempos no JSON são medidos localmente e incluem startup, downloads se
necessários e a falha injetada: não são um benchmark de throughput do Kafka.

## Evidências e preservação

Cada execução cria `work/kafka-validation/<data-id>/`, com `result.json`, amostra
publicada, Delta Silver/Gold, DLQ, checkpoints e progresso das consultas. Não
modifica `work/lakehouse`, o caminho Delta nativo, o CSV ou tópicos existentes.
Um caminho `--output` existente é recusado, evitando mistura de execuções.
O tópico exclusivo permanece no broker para inspeção. O script não remove
tópicos nem volumes automaticamente.

O producer e cliente de administração desta validação usam IPv4; a JVM é
iniciada com preferência por IPv4 para o endereço `localhost` anunciado pelo
Compose. O comando limita a amostra a 1.000 linhas por configuração e o sink
continua coletando o microbatch no driver, adequado somente à demonstração local.

## Garantias e pendências

`foreachBatch` pode ser executado novamente: a garantia é entrega pelo menos uma
vez com efeitos idempotentes no sink versionado, não exatamente uma vez entre
Kafka, Delta, Gold e DLQ. A DLQ e o sink têm checkpoints independentes. Retenção
do tópico e compatibilidade dos checkpoints precisam ser preservadas. Não
altere tópico/checkpoint de uma consulta já iniciada. A watermark de dois dias
descarta correções antigas; reconciliação batch é necessária.

Continuam pendentes: queda do broker/VM, kill abrupto do processo, rebalance com
múltiplos brokers, expiração de offsets, escala distribuída e execução Airflow.
O teste controlado de exceção não demonstra esses cenários. A integração Kafka
foi posteriormente aprovada no cenário local documentado abaixo; os demais
cenários desta seção continuam pendentes.

## Resultado posterior fornecido pelo usuário: aprovado no Mac

Em 06/11/2026, o usuário enviou o transcript completo de execução em macOS
13.7.8 Intel, Python 3.12.13, Spark 3.5.8 e Java 17 selecionado no shell.
O pytest terminou com **7 passed, 142 warnings in 43.72s**.
O runner terminou com **status: passed**, com os dez checks verdadeiros.

- CSV SHA256: `7d26e6f4528c2ca2636a8fea2c5bc8755a0ff6a182d82c238a8793b016e4366d`.
- Até 20 registros solicitados; **9** disponíveis na seleção Ceará de dois dias.
- Primeira publicação: **11 mensagens** (9 reais + 1 duplicata + 1 envelope inválido).
- Silver: **9 linhas**; DLQ: **1**; uma duplicata detectada no primeiro microbatch.
- Segunda publicação: **2 mensagens**, incluindo tombstone sintético versão 2.
- Falha controlada observada após commit; recuperação retomou offsets **11 → 13**.
- Gold após tombstone: **8 acidentes ativos**.
- Reinício vazio: offsets **13 → 13**, zero entradas consumidas.
- Tempo total do comando: **52.640601533988956 s**, incluindo resolução/download
  do conector, startup e falha controlada; não é throughput sustentado.

A cópia para GitHub inclui o JSON e a proveniência com hash em
`evidence/kafka-validation-20261106/mac-user-run/`. Os caminhos no JSON são
relativos ao repositório. As métricas foram preservadas.
O resultado foi validado a partir do log fornecido; os diretórios do Mac não
foram inspecionados diretamente pelo assistente. A queda de broker/processo,
multinó, retenção de offsets e Airflow não foram demonstrados por esta execução.
