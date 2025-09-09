# demodados
## Projeto
Levantamento de dados para análises políticas  com foco em temas de cidadania, democracia e advocacy no Brasil.\
Este projeto utiliza Python, Airflow, PostgreSQL e dbt para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

## Estrutura

**[demodados](https://github.com/lksprado/demodados)** Repositório que contém configuração do Banco de Dados e Códigos de Pipelines para Ingestão\
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** Repositório que os modelos de dados com dbt para o DW\
**[Orquestrador](https://github.com/lksprado/demodados_orq)** Repositório que contém as DAG para execução do projeto de dados

`./src` - Códigos-fonte para Extração, Transformação e Carga de dados. [Ir](https://github.com/lksprado/demodados/tree/main/src)\
`./src/utils/` - Métodos e Classes reutilizáveis. [Ir](https://github.com/lksprado/demodados/tree/main/src/utils)\
`./src/pipelines/` - Lógica de execução de pipelines e schema de modelos para carga. [Ir](https://github.com/lksprado/demodados/tree/main/src/pipelines)\
`./demodadosDw` - Modelos de dados com dbt para DW no PostgresSQL\

## Escopos
Atualizado em 05/08/2025
- Câmara dos Deputados (API oficial)
- Radar Congresso (Webscraping)

## Arquitetura de Dados
O projeto segue a arquitetura de camadas em Data Lakehouse:
- **Landing**: Arquivos originais, preservado o formato de saída da Fonte
- **Bronze**: Dados estruturas em tabelas raw no PostgresSQL
- **Silver (dbt)**: dados limpos e padronizados
- **Gold (dbt)**: datasets analíticos prontos para consumo

## Ferramentas
| Ferramenta    | Uso                                          |
|---------------|----------------------------------------------|
| Python        | Pipelines de ETL                             |
| Airflow       | Orquestração e agendamento de pipelines      |
| PostgreSQL    | Armazenamento e versionamento dos dados      |
| dbt           | Modelagem e documentação dos dados           |
| Pandera       | Validação de schema para carga raw           |
