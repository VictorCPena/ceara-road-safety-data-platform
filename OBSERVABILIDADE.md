# Observabilidade local do Lakehouse

O exportador lê o relatório final mais recente produzido por
scripts/run_airflow_pro.py no volume de trabalho do Airflow. O Prometheus
coleta as métricas a cada 5 segundos. O Grafana consulta esse Prometheus,
e o Alertmanager envia as notificações a um receptor HTTP local.

O perfil reaproveita a imagem Python que já foi construída para o Airflow.
Os dois serviços Python usam apenas a biblioteca padrão. O volume do
Airflow é externo ao Compose de monitoramento e montado para leitura.
O nome padrão corresponde ao stack executado no Mac. Para outra instalação,
defina LAKEHOUSE_AIRFLOW_WORK_VOLUME com o nome do volume existente.

| Sinal | Interpretação |
|---|---|
| Última execução verificada | Resultado registrado pelo runner, incluindo a conferência dos dados |
| Aceitos e quarentena | Contagens do último relatório aprovado |
| Bronze, Silver, Gold e fato | Contagem nas tabelas e total de acidentes na agregação Gold |
| Modelos e testes dbt | Resultados da conferência feita após o DAG |
| Duração | Tempo do runner e da verificação, em segundos |
| Idade do relatório | Tempo desde a gravação do último resultado completo |
| Scrape | Disponibilidade do endpoint do exportador para o Prometheus |

O estado observado é o do último relatório finalizado pelo runner. O disparo
pela interface do Airflow, sozinho, não gera esse relatório. O painel mostra
snapshots coletados desde a subida do Prometheus. Os relatórios antigos
servem de ponto inicial para a leitura atual, sem preencher séries históricas
retroativamente. A quarentena é um aviso de qualidade, mesmo quando o
pipeline passou.

## Executar no Mac

Use o diretório do Lakehouse com o volume Airflow já existente e a imagem
ceara-lakehouse-airflow-pro:3.3.2-py312 presente no Docker.

```bash
cd "$HOME/Downloads/portfolio-internacional-dados-ia/ceara-road-safety-lakehouse-pro" &&
docker compose -f compose.observability.yaml config --quiet &&
docker compose -f compose.observability.yaml run --rm --no-deps \
  --entrypoint /bin/promtool prometheus check config prometheus.yml &&
docker compose -f compose.observability.yaml run --rm --no-deps \
  --entrypoint /bin/promtool prometheus test rules alerts.test.yml &&
docker compose -f compose.observability.yaml up -d --no-build &&
python3.12 scripts/check_observability.py
```

A primeira subida baixa as imagens de Prometheus, Alertmanager e Grafana.
O verificador espera o aviso de quarentena entrar em firing após um minuto,
confere o alerta ativo no Alertmanager e exige um recibo do webhook com o
mesmo horário de início. Também lê o dashboard e consulta o datasource
através do Grafana. O JSON final tem status passed apenas se a cadeia passar.

O relatório já registrado no Mac tem 73.202 registros Bronze, 73.201 aceitos
e um em quarentena. Ele permite exercitar o aviso sem executar o Spark de
novo nem inserir linhas artificiais no volume do Airflow. Se houver um
relatório posterior, o painel usa esse resultado e o teste de demonstração
requer que ele tenha passado e contenha uma quarentena.

| Interface | Endereço padrão |
|---|---|
| Grafana | http://localhost:13000/d/prf-airflow |
| Prometheus | http://localhost:19090 |
| Alertmanager | http://localhost:19093 |
| Recibos locais | http://localhost:19108/notifications |

O Grafana abre como Viewer na instalação local. As portas publicadas ficam
no loopback. O receptor mantém os últimos 64 eventos em memória e imprime
recibos no log. O Alertmanager repete notificações a cada minuto neste perfil
de demonstração, inclusive para que um receptor reiniciado volte a receber
um alerta que ainda esteja ativo. A retenção do Prometheus é de três dias.

Se uma porta estiver ocupada, use a variável correspondente no shell que
chama o Compose e o verificador. As opções são PRF_OBS_GRAFANA_PORT,
PRF_OBS_PROMETHEUS_PORT, PRF_OBS_ALERTMANAGER_PORT e PRF_OBS_RECEIVER_PORT.

Para investigar uma subida incompleta:

```bash
docker compose -f compose.observability.yaml ps -a
docker compose -f compose.observability.yaml logs --tail=50 \
  exporter prometheus alertmanager receiver grafana
```

Para parar o monitoramento e manter os volumes:

```bash
docker compose -f compose.observability.yaml stop
```

## Alertas

| Regra | Condição | Espera |
|---|---|---|
| PRFQuarantineNonzero | Relatório aprovado com pelo menos um registro em quarentena | 1 min |
| LakehouseLatestRunFailed | Último relatório disponível com falha | 30 s |
| LakehouseReportUnavailable | Relatório ausente, corrompido ou incompatível | 1 min |
| LakehouseExporterDown | Prometheus não consegue coletar o alvo | 30 s |

Os cenários de teste do promtool usam séries sintéticas, isoladas do CSV real
e dos relatórios de produção local. Alertas e recebimento via webhook são
locais. Integrações de e-mail e mensageria estão fora desse perfil.

## Evidência de 09/11/2026

Quatro testes Python passaram, incluindo requisições HTTP reais ao exportador
e ao receptor. A integração HTTP local também leu uma cópia do relatório
real de 06/11, com 73.201 aceitos, uma quarentena e sete testes dbt.
O webhook dessa etapa recebeu um payload de teste. Os resultados locais
estão em evidence/observability-20261109/.

## Execução integrada no Mac

Em 09/11/2026, os comandos do usuário validaram a configuração e o arquivo
com quatro regras. A suíte sintética de regras passou no promtool. Os cinco
serviços iniciaram e o verificador retornou status passed.

O scrape leu 73.202 linhas Bronze, 73.201 aceitas, uma quarentena, três modelos
e sete testes dbt. O dashboard prf-airflow e a consulta pelo datasource do
Grafana responderam pela API. O Alertmanager mostrou PRFQuarantineNonzero
ativo e o receptor registrou o webhook em firing às 14:12:53 em Fortaleza.
A diferença entre o timestamp de início do alerta e o recibo foi 5,041196 s
nesta execução. A prova inclui uma entrega local do alerta de quarentena.
A cobertura das demais regras vem da suíte sintética do promtool.

O JSON e a proveniência estão em evidence/observability-20261109/mac-run/.
O material foi transcrito da saída fornecida pelo usuário.

Referências técnicas consultadas. [Testes de regras do Prometheus](https://prometheus.io/docs/prometheus/3.9/configuration/unit_testing_rules/) · [Payload de notificações do Alertmanager](https://github.com/prometheus/alertmanager/blob/main/docs/notifications.md) · [API de datasource do Grafana](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/data_source/) · [Configuração do Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/)
