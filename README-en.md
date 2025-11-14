# demodados
[Versão em Português](https://github.com/lksprado/demodados/tree/main/README.md)

## What Is This Project?
Data collection for political analysis focused on citizenship, democracy, and advocacy in Brazil.\
This project uses Python, Airflow, PostgreSQL, and dbt to structure, model, and publish public data in an accessible and reliable way.

This repository deals exclusively with the Ingestion process.

## Project Repositories Structure

**[Ingestor](https://github.com/lksprado/demodados)** (this repo): ingestion pipelines -> produces Raw and Bronze layers.\
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** Repo for SQL modeling with dbt -> produces Silver and Gold in the DW.\
**[Orchestrator](https://github.com/lksprado/demodados_orq)** Repo for orchestration.

## Project Tools
| Tool         | Purpose                                      |
|--------------|----------------------------------------------|
| Python       | ETL pipelines                                 |
| Airflow      | Pipeline orchestration and scheduling         |
| PostgreSQL   | Data storage and versioning                   |
| dbt          | Data modeling and documentation               |
| Pandera      | Schema validation for raw loads               |

## demodados Repository Structure

`./src` - Source code for Extract, Transform, and Load. [Go](https://github.com/lksprado/demodados/tree/main/src)\
`./src/utils/` - Reusable methods and classes. [Go](https://github.com/lksprado/demodados/tree/main/src/utils)\
`./src/pipelines/` - Pipeline execution logic and model schemas for loading. [Go](https://github.com/lksprado/demodados/tree/main/src/pipelines)\
`./src/params/` - Files for recursive parameterization, e.g., IDs for URLs. [Go](https://github.com/lksprado/demodados/tree/main/src/params)

## Data Sources
Updated on 2025-10-26
- Chamber of Deputies (official API)
- Radar Congresso (web scraping)
- Federal Senate (official API)
- Ranking Políticos (web scraping)

## Ingestion Architecture
The project as a whole follows a layered Data Lakehouse architecture. So far we have:
- Raw: Raw files in the source’s native output format.
- Bronze: Preprocessed tabular files for loading into the database raw schema.

## Getting Started
Ensure you have Python 3.12.1 and the `uv` package manager.

```
# Clone the repository
git clone https://github.com/lksprado/demodados.git
cd demodados

# Create and activate the virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv install -r requirements.txt

# Run an example pipeline
python -m src.pipelines.legislativo.parlamento_deputados

```

## Visualization
Analysis: https://www.linkedin.com/posts/activity-7388738017097646080-hrC2?utm_source=share&utm_medium=member_desktop&rcm=ACoAABe9KIQBnSGcGFjCIZscCTyc7RManCDQlqU
![Deputies and Senators](<images/Parliamentarians English.png>)
