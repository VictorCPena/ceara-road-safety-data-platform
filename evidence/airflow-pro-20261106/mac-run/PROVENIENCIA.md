# Proveniência

JSON transcrito da mensagem de terminal fornecida pelo usuário em 06/11/2026, sem alterar métricas. Não foi baixado diretamente do Mac nem reexecutado pelo assistente. O usuário mostrou `airflow-init` com Exited (0), PostgreSQL/API saudáveis, porta localhost:18080 e run passando por queued → running → success. Scheduler e DAG processor ainda apareciam health: starting na listagem anterior ao run; o sucesso das tarefas não deve ser apresentado como captura posterior dos healthchecks.

Run: `portfolio_20261106T212429_10ecd65d`. Início 21:24:29 UTC = 18:24:29 America/Fortaleza. Tempo reportado: 159.46772988299927 segundos, incluindo acompanhamento do run e validação; não é latência de API ou benchmark de throughput. Dados PRF reais; nenhuma linha sintética neste run. Validação é local, um broker separado e um nó; não comprova alta disponibilidade nem recuperação de falha durante tarefa.
