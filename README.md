# demodados
[English Version](https://github.com/lksprado/demodados/tree/main/README-en.md)

## O que é este projeto?
Levantamento de dados para análises políticas  com foco em temas de cidadania, democracia e advocacy no Brasil.\
Este projeto utiliza Python, Airflow, PostgreSQL e dbt para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

**Este repositório trata exclusivamente do processo de Ingestão.**


## Estrutura de Repositórios  do Projeto

**[Ingestor](https://github.com/lksprado/demodados)** (Este repo): pipelines de ingestão -> gera camadas Raw e Bronze.\
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** Repo para modelagem SQL com dbt -> gera Silver e Gold no DW.\
**[Orquestrador](https://github.com/lksprado/demodados_orq)** Repo para orquestração.

## Ferramentas do Projeto
| Ferramenta    | Uso                                          |
|---------------|----------------------------------------------|
| Python        | Pipelines de ETL                             |
| Airflow       | Orquestração e agendamento de pipelines      |
| PostgreSQL    | Armazenamento e versionamento dos dados      |
| dbt           | Modelagem e documentação dos dados           |
| Pandera       | Validação de schema para carga raw           |


## Estrutura Repositório demodados

`./src` - Códigos-fonte para Extração, Transformação e Carga de dados. [Ir](https://github.com/lksprado/demodados/tree/main/src)\
`./src/utils/` - Métodos e Classes reutilizáveis. [Ir](https://github.com/lksprado/demodados/tree/main/src/utils)\
`./src/pipelines/` - Lógica de execução de pipelines e schema de modelos para carga. [Ir](https://github.com/lksprado/demodados/tree/main/src/pipelines)\
`./src/params/`- Arquivos para parametrização recursiva. Ex: IDs para URLs. [Ir](https://github.com/lksprado/demodados/tree/main/src/params)

## Fontes de Dados
Atualizado em 16/11/2025
- Câmara dos Deputados (API oficial)
- Radar Congresso (Webscraping)
- Senado Federal (API oficial)
- Ranking Políticos (Webscraping)
- E-Cidadania (Webscraping)

## Arquitetura de Ingestão
O projeto como um todo segue a arquitetura de camadas em Data Lakehouse. Até aqui temos:
- **Raw**: Arquivos brutos no formato de saída da Fonte.
- **Bronze**: Arquivos pré-processados em formato tabular para Carga em schema raw do database.

## Começando
Certique ter o Python 3.12.1 e gerenciador de pacote `uv`.

```
# Clone o repositório
git clone https://github.com/lksprado/demodados.git
cd demodados

# Crie e ative o ambiente virtual
uv venv
source .venv/bin/activate

# Instale dependências
uv install -r requirements.txt

# Execute uma pipeline de exemplo
python -m src.pipelines.legislativo.parlamento_deputados

```

## Visualização
Análise: https://www.linkedin.com/posts/activity-7388738017097646080-hrC2?utm_source=share&utm_medium=member_desktop&rcm=ACoAABe9KIQBnSGcGFjCIZscCTyc7RManCDQlqU
![Deputados e Senadores](<images/Parliamentarians PTBR.png>)
Análise: https://www.linkedin.com/posts/activity-7394749150577442817-UrNh?utm_source=share&utm_medium=member_desktop&rcm=ACoAABe9KIQBnSGcGFjCIZscCTyc7RManCDQlqU
![Consulta](<images/Parliamentarians Tool PTBR.png>)
