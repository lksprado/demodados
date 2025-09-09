# demodados
## Projeto
Levantamento de dados para análises políticas  com foco em temas de cidadania, democracia e advocacy no Brasil.\
Este projeto utiliza Python, Airflow, PostgreSQL e dbt para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

Este repositório trata exclusivamente do processo de Ingestão..


## Estrutura Completa do Projeto

**[demodados](https://github.com/lksprado/demodados)** Repositório que contém configuração do Banco de Dados e códigos de Pipelines para Ingestão. <<< *Você está aqui!*\
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** Repositório para os modelos de dados SQL com dbt para o DW.\
**[Orquestrador](https://github.com/lksprado/demodados_orq)** Repositório que contém as DAG para execução das pipelines com Airflow.

## Ferramentas do Projeto
| Ferramenta    | Uso                                          |
|---------------|----------------------------------------------|
| Python        | Pipelines de ETL                             |
| Airflow       | Orquestração e agendamento de pipelines      |
| PostgreSQL    | Armazenamento e versionamento dos dados      |
| dbt           | Modelagem e documentação dos dados           |
| Pandera       | Validação de schema para carga raw           |


## Estrutura Repositório demodados
Comece daqui para implementar pipelines de dados seguindo a documentação.

`./src` - Códigos-fonte para Extração, Transformação e Carga de dados. [Ir](https://github.com/lksprado/demodados/tree/main/src)\
`./src/utils/` - Métodos e Classes reutilizáveis. [Ir](https://github.com/lksprado/demodados/tree/main/src/utils)\
`./src/pipelines/` - Lógica de execução de pipelines e schema de modelos para carga. [Ir](https://github.com/lksprado/demodados/tree/main/src/pipelines)\
`./src/params/`- Arquivos para parametrização recursiva. Ex: IDs para URLs. [Ir](https://github.com/lksprado/demodados/tree/main/src/params)

## Fontes de Dados
Atualizado em 09/09/2025
- Câmara dos Deputados (API oficial)
- Radar Congresso (Webscraping)
- Senado Federal (API oficial)

## Arquitetura de Ingestão
O projeto como um todo segue a arquitetura de camadas em Data Lakehouse. Até aqui temos:
- **Raw**: Arquivos brutos no formato de saída da Fonte.
- **Bronze**: Arquivos pré-processados em formato tabular para Carga em schema raw do database.
