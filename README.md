# data-platform

Plataforma de dados local construída com ferramentas open source, simulando uma arquitetura de mercado real em ambiente de desenvolvimento.

> Projeto de portfólio — Engenharia de Dados | Daniel Santos · 2026

---

## O que foi construído

Dois pipelines independentes rodando sobre a mesma infraestrutura containerizada:

**Pipeline E-Commerce (batch diário)**
Dados reais da Olist passam pelas camadas Bronze → Silver → Gold em formato Parquet no MinIO, orquestradas pelo Airflow e expostas via Metabase.

**Pipeline Financeiro (near real-time)**
Cotações de AAPL, MSFT e HSIC coletadas via Yahoo Finance a cada 15 minutos, armazenadas no PostgreSQL e visualizadas com auto-refresh no Metabase.

---

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 3.2.2 |
| Object Storage | MinIO (S3-compatible) |
| Processamento | DuckDB + Python 3.11 |
| Banco de dados | PostgreSQL 16 |
| Catalogação | Unity Catalog Open Source |
| Visualização | Metabase |
| Infraestrutura | Docker Compose |

---

## Arquitetura

```
E-Commerce (Olist)
  Kaggle → Bronze (Parquet) → Silver (Parquet) → Gold (Parquet) → Metabase
                MinIO          MinIO               MinIO

Financeiro (Yahoo Finance)
  yfinance → Airflow DAG (*/15 min) → PostgreSQL → Metabase
```

Todas as camadas são registradas no Unity Catalog com metadados, lineage e localização dos arquivos.

---

## Serviços

| Serviço | URL |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Unity Catalog | http://localhost:8081 |
| Unity Catalog UI | http://localhost:3001 |

---

## Como rodar

**Pré-requisitos:** Docker, Python 3.11, 8 GB RAM

```bash
# 1. Clonar
git clone https://github.com/danielbattdev/data-platform.git
cd data-platform

# 2. Configurar variáveis
cp .env.example docker/.env
# edite docker/.env com suas chaves

# 3. Subir infraestrutura
cd docker && docker compose up -d

# 4. Criar virtualenv
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Pipeline E-Commerce (manual):**
```bash
python duckdb/scripts/ingest.py
python duckdb/scripts/transform_silver.py
python duckdb/scripts/transform_gold.py
```

**Pipeline Financeiro (manual):**
```bash
python scripts/collect_stocks.py
```

**Via Airflow:**
```bash
# Trigger manual de qualquer DAG
docker exec docker-airflow-scheduler-1 airflow dags trigger <dag_id>
```

---

## DAGs

| DAG | Schedule | Descrição |
|---|---|---|
| `pipeline_olist` | Diário às 6h | Bronze → Silver → Gold |
| `collect_stocks_15min` | A cada 15 min | Yahoo Finance → PostgreSQL |

---

## Dataset E-Commerce

Brazilian E-Commerce Public Dataset — [Olist (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

- 99.441 pedidos · período 2016–2018
- 9 arquivos CSV: pedidos, clientes, produtos, vendedores, pagamentos
- Licença: CC BY-NC-SA 4.0

---

## Estrutura

```
data-platform/
├── docker/
│   ├── dags/
│   │   └── dag_collect_stocks.py
│   └── docker-compose.yml
├── duckdb/
│   └── scripts/
│       ├── ingest.py
│       ├── transform_silver.py
│       └── transform_gold.py
├── catalog/
│   └── schemas/
│       └── register_tables.py
├── scripts/
│   └── collect_stocks.py
├── tests/
├── .env.example
└── requirements.txt
```

---

## Equivalência corporativa

| Este projeto | Produção |
|---|---|
| MinIO | AWS S3 / Azure ADLS Gen2 |
| DuckDB | Apache Spark / Trino |
| Airflow local | Cloud Composer / MWAA / Astronomer |
| Metabase CE | Power BI / Tableau / Looker |
| Unity Catalog OS | Unity Catalog Enterprise / Datahub |
| yfinance | Bloomberg API / Refinitiv |
| PostgreSQL local | AWS RDS / Azure Database |

---

## Autor

Daniel Santos · [@danielbattdev](https://github.com/danielbattdev)
