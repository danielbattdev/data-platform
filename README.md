# Data Platform — Portfolio Project

Plataforma de dados local com arquitetura Medallion (Bronze/Silver/Gold), construída com ferramentas open source.

## Stack

| Componente | Tecnologia | Função |
|------------|-----------|--------|
| Storage | MinIO (S3-compatible) | Object storage — camadas Bronze/Silver/Gold em Parquet |
| Processamento | DuckDB + Python | Transformações analíticas |
| Orquestração | Apache Airflow 3.2.2 | Agendamento e execução do pipeline |
| Catalogação | Unity Catalog OS | Governança e descoberta de dados |
| Visualização | Metabase | Dashboards e KPIs |

## Dataset

Brazilian E-Commerce (Olist) via Kaggle — 99.441 pedidos reais (2016-2018).

## Arquitetura
Kaggle CSV → Bronze (Parquet) → Silver (limpo) → Gold (KPIs) → Metabase

↑

Unity Catalog
## Como Executar

```bash
# 1. Subir infraestrutura
docker compose up -d

# 2. Ativar ambiente Python
source .venv/bin/activate

# 3. Executar pipeline
python duckdb/scripts/ingest.py
python duckdb/scripts/transform_silver.py
python duckdb/scripts/transform_gold.py

# 4. Executar testes
python tests/test_pipeline.py

# 5. Acessar serviços
# Airflow:       http://localhost:8080
# MinIO:         http://localhost:9001
# Metabase:      http://localhost:3000
# Unity Catalog: http://localhost:8081
```

## Resultados

- 99.441 pedidos processados
- 25 meses de dados analisados
- Média de entrega: 14,3 dias
- Pipeline executando diariamente via Airflow
