# Vendas de Carros ETL Pipeline
## Objetivo:
- Desenvolver um pipeline ETL automatizado para dados automotivos históricos (1992–2025) com análise de tendência.

## Dataset:
- Os dados são CSVs de cada ano (1992–2025) hospedados no GitHub.

## Tecnologias:
- Python
- pandas
- matplotlib
- GitHub (como fonte de dados)

## ETL:
- Extract — extração a partir de links RAW do GitHub
- Transform — unificação, limpeza e padronização
- Load — geração de arquivo consolidado + visualizações

## Visualizações:
- Número de modelos por ano
- Distribuição de modelos por década


## Como rodar:
- git clone https://github.com/PedroBZR12/vendas-de-carros-etl-pipeline.git
- cd us-automotive-etl
- python pipeline.py

## Próximos passos:
- Inserção em banco PostgreSQL


