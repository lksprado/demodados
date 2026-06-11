# Visão Geral
Este projeto trata-se de pipelines de ingestão de dados de diferentes origens sobre dados políticos.
A extração de dados pode ser por uso de API's ou Webscraping.
A carga de dados será para um Postgres local.
O ETL pode ser feito localmente como teste ou em produção com Airflow.

# Objetivos
Alimentar a camada Raw que será trabalhada em um Data Warehouse através de DAGs do Airflow.
Este repositório serve como submódulo em um repositório Airflow para elaboração de DAGs.

# Estrutura
local_setup/src/pipelines
/camara
/ecidadania
/radar_congresso
/senado
/ranking_politicos
