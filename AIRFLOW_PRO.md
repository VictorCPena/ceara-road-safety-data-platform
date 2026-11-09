# Airflow + PostgreSQL: execução local

## O que foi entregue

`compose.airflow-pro.yaml` é um stack separado do Compose original e do Redpanda.
Executa PostgreSQL 17, migração do banco Airflow, API/UI, scheduler LocalExecutor
e processador de DAGs. Apenas o DAG `lakehouse_pro` é montado nesse perfil.
O DAG original ampliado permanece disponível no Compose original.

O DAG executa `batch_prf` → `dbt_quality`, com uma tarefa ativa por vez,
timeouts, retries no batch e bloqueio da análise quando a tarefa anterior falha.
A imagem usa Airflow 3.3.2/Python 3.12 e Java 17 copiado de Temurin, com um venv
isolado para Spark/Delta/dbt. O runtime Airflow não recebe as dependências do
projeto no próprio ambiente. Não é necessário instalar Airflow no venv do Mac.

Os volumes `airflow_pro_postgres`, `airflow_pro_runtime` e `airflow_pro_work`
ficam no projeto Compose `ceara-lakehouse-airflow-pro`. Os dados batch gerados
anteriormente no host e os checkpoints/tópicos Kafka não são usados como destino
de gravação deste perfil. `./data` é montado somente para leitura. Os resultados
orquestrados e artefatos dbt são compartilhados entre os serviços em `/app/work`.

## Preparar e construir

Colima/Docker precisam estar rodando; o CSV deve existir. A configuração anterior
do usuário usa 2 CPUs e 4 GiB na VM. Uma tarefa por vez e um worker da API reduzem
o consumo, mas não há medição de memória dos contêineres ainda. O primeiro build
baixa imagens Java/Airflow/PostgreSQL e dependências Python, podendo demorar.
Se faltar memória, examine os estados/OOM antes de alterar a VM ou apagar dados.

```bash
cd "$HOME/Downloads/portfolio-internacional-dados-ia/ceara-road-safety-lakehouse-pro"
source .venv312/bin/activate
python scripts/prepare_airflow_pro.py
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml config --quiet
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml build airflow-api-server
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml up -d --no-build \
  postgres airflow-api-server airflow-scheduler airflow-dag-processor
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml ps -a
```

Em uma VM pequena, pode-se parar apenas o broker enquanto valida Airflow:
`docker compose -f compose.portfolio.yaml stop redpanda`. Isso preserva o volume
e os tópicos. Depois, `docker compose -f compose.portfolio.yaml up -d redpanda`
retoma o broker. O teste Airflow batch/dbt não depende do Redpanda.

`prepare_airflow_pro.py` cria `.env.airflow-pro` com modo 0600 e segredos aleatórios
para PostgreSQL, JWT, API e Fernet. Reexecuções preservam os valores existentes;
arquivos incompletos/symlinks são recusados. Não é uma rotação de credenciais.
O arquivo é excluído do Docker build e do ZIP. Não execute `compose config` sem
`--quiet` para compartilhar a saída: a configuração expandida contém segredos.

## Executar uma DAG e validar os dados

```bash
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml exec -T \
  airflow-api-server python /app/scripts/run_airflow_pro.py
```

O comando lê a senha local do SimpleAuthManager dentro do volume Airflow,
obtém JWT sem exibi-lo, aguarda o registro do DAG, despausa e dispara um run único
pela API REST autenticada. O **scheduler** executa as duas tarefas. O script
aguarda até 15 minutos e exige DAG e ambas as tarefas em `success`.

Depois confere o hash do CSV, o marcador do batch, as contagens Bronze/Silver/Gold,
a tabela fato e os totais analíticos DuckDB, mais três modelos e sete testes dbt.
As leituras de validação não alteram os dados. Apenas após essas verificações o
resultado final é `status: passed`. Erro/timeout deixa um resultado `failed` e
preserva a execução para investigação; timeout não cancela automaticamente a DAG.

Evidência: `/app/work/airflow-runs/<run_id>/result.json` no volume compartilhado,
além dos logs e artefatos em `/app/work/dbt-target/`. O comando imprime JSON
sem senha/token. Para copiar um resultado específico ao host, use `docker cp`
com o contêiner de API e esse caminho; não copie o volume de segredos para o ZIP.

## Interface e operação

UI: http://localhost:18080 por padrão, usuário `admin`. Pode-se definir
`AIRFLOW_PRO_PORT` para outra porta livre; o bind e a URL pública usam a mesma
variável. A porta interna continua 8080. A senha é gerada pelo Airflow e fica
no arquivo `simple_auth_manager_passwords.json.generated` do volume runtime.
Para consultá-la localmente:

```bash
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml exec -T \
  airflow-api-server python -c 'import json,os; from pathlib import Path; print(json.loads((Path(os.environ["AIRFLOW_HOME"])/"simple_auth_manager_passwords.json.generated").read_text())["admin"])'
```

Não compartilhe a saída desse comando. O SimpleAuthManager é destinado ao
desenvolvimento/teste; este perfil usa bind somente localhost, sem publicação.
PostgreSQL não expõe porta no host. O scheduler tem hostname estável para os
links de logs servidos. Não há triggerer porque este DAG não usa operadores
deferíveis, nem Redis/Celery porque o executor é local.

Para parar sem remover dados:

```bash
docker compose --env-file .env.airflow-pro -f compose.airflow-pro.yaml stop
```

Não use `down -v`, não apague `.env.airflow-pro` depois de inicializar PostgreSQL
e não remova volumes como tentativa genérica de correção. PostgreSQL mantém a
senha da primeira inicialização; gerar outra senha no host não altera o usuário
já existente no banco. Preserve os arquivos para uma recuperação orientada pelo
erro real.

## Validações executadas em 06/11/2026

- Import do DAG original aprovado com Airflow 3.3.2 antes do teste da atualização.
- Cinco testes aprovados: ordem/guardrail do DAG, configuração isolada,
  preservação/permissões dos segredos, recusa de symlink e erro HTTP sem vazar
  corpo de autenticação. PyYAML testa a estrutura; **não equivale a Compose real**.
- Airflow 3.3.2 executou `dag.test()` com metadados SQLite e uma **fixture sintética**
  de três linhas: duas aceitas e uma em quarentena. As duas tarefas passaram;
  dbt criou três modelos e aprovou sete testes. Execução final: 17,07124 s conforme
  o registro DagRun. Esse tempo não é um benchmark de produção.
- Validação cruzada dos artefatos aprovada. Contagens analíticas inconsistentes
  e hash de CSV incompatível foram introduzidos na cópia de teste e rejeitados;
  a fixture foi restaurada e validada novamente.
- Ruff aprovou os arquivos novos/alterados. O ambiente de projeto usado no QA
  passou `pip check`.

Durante QA, a leitura dos artefatos encontrou que `dbt --target-path` relativo
era resolvido em relação ao diretório do projeto dbt, fora do volume esperado.
O DAG foi corrigido para usar caminhos absolutos derivados de `PROJECT`, e a
execução completa e a validação de dados foram repetidas com sucesso. As tentativas
de preparar o harness (Pandas ausente no venv Airflow e DAG fora do bundle)
também estão registradas como falhas de preparação, não testes aprovados.

Logs em `evidence/airflow-pro-20261106/`. Ambiente: Linux x86_64, Python 3.12.14,
Java 21.0.13 no QA; **o contêiner entregue seleciona Java 17 e ainda não foi
construído neste ambiente**. O QA SQLite não comprova execução pelo scheduler
com PostgreSQL, transporte HTTP do helper ou permissões dos volumes Docker.

**Pendentes até execução no Mac:** build da imagem, Compose real, migração
PostgreSQL, healthchecks dos serviços, autenticação REST no contêiner, disparo
pelo scheduler com CSV real e recuperação após falhas desses serviços.

## Referências oficiais consultadas em 06/11/2026

- https://airflow.apache.org/docs/apache-airflow/3.3.2/security/api.html
- https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html
- https://airflow.apache.org/docs/apache-airflow/3.3.2/cli-and-env-variables-ref.html

## Atualização: build real no Mac e conflito de porta

O usuário forneceu o log em 06/11/2026: build aprovado, imagem
`ceara-lakehouse-airflow-pro:3.3.2-py312` (`f75ba8f12209`), 1639,8 s reportados
pelo Compose, incluindo downloads/builds de dependências. `pip check` e os imports
de build passaram; PostgreSQL ficou saudável. A API não iniciou porque a porta
8080 do host estava ocupada. O scheduler não executou a DAG. A medição não é
benchmark de treinamento/inferência.

A correção troca apenas a publicação para localhost:18080 (ou `AIRFLOW_PRO_PORT`)
e a URL pública. A porta interna permanece 8080, inclusive healthcheck,
Execution API e helper REST. Não é necessário reconstruir a imagem. Suba de novo
com `up -d --no-build` após a correção e preserve as credenciais/volumes existentes.
O resumo de build e dos cinco testes da configuração (1,19 s) está em
`evidence/airflow-pro-20261106/mac-build/`. A correção ainda requer execução real
no Mac; os limites anteriores referentes ao build são históricos e foram
superados por esta evidência, enquanto o DAG com PostgreSQL continua pendente.

## Resultado posterior: DAG com PostgreSQL aprovado no Mac

Após corrigir a porta para 18080, o usuário mostrou init Exited (0), PostgreSQL
e API saudáveis, e run `portfolio_20261106T212429_10ecd65d` passando por
queued → running → success. O resultado final foi `status: passed`, com
`batch_prf` e `dbt_quality` em success. O verificador conciliou o CSV real,
Bronze 73.202, Silver/Gold/fato 73.201 e uma quarentena, além de três modelos
e sete testes dbt. Duração reportada do run e validação: 159.46772988299927 s.
A execução começou em 06/11/2026 às 18:24:29 em America/Fortaleza (21:24:29 UTC).

O JSON transcrito e sua proveniência estão em
`evidence/airflow-pro-20261106/mac-run/`. Isso supera as pendências históricas
de build, Compose, migração PostgreSQL, autenticação REST e disparo pelo scheduler
listadas nas seções anteriores. Recuperação após reinício/falha dos serviços,
crash durante tarefa e execução distribuída continuam sem validação. Não houve
reexecução deste run pelo assistente nem inspeção direta dos volumes no Mac.

## Reinício programado do scheduler e replay aprovados

O usuário executou `restart airflow-scheduler` e disparou uma nova DAG pela API.
O run `portfolio_20261106T214640_559df1a9`, iniciado às 18:46:40 em Fortaleza
(21:46:40 UTC) em 06/11/2026, chegou a success; ambas as tarefas e a conferência
de dados passaram. O hash e todas as contagens coincidem com o run anterior:
Bronze 73.202, Silver/Gold/fato 73.201, uma quarentena, três modelos e sete testes.
Isso comprova replay sem duplicar as camadas nesse cenário local após reinício
programado do scheduler. Duração reportada: 128.0716736029999 s, sem estudo
controlado que permita atribuir diferença de tempo a otimização.

JSON e comparação em `evidence/airflow-pro-20261106/mac-scheduler-restart/`,
transcritos da mensagem do usuário. Crash durante tarefa e reinícios de banco/API
não foram executados. Não é demonstração de alta disponibilidade.
