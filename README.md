# Data Platform — Henry Schein Brasil

Plataforma de dados construída com ferramentas open source, implementando uma arquitetura de mercado real em ambiente local.

**Autor:** Daniel Santos · Henry Schein Brasil · Junho 2026

---

## Pipelines

### E-Commerce — Batch Diário
Dados reais da Olist percorrem três camadas de refinamento até chegar ao Metabase.

```
Kaggle → Bronze (Parquet) → Silver (Parquet) → Gold (Parquet) → Metabase
                    MinIO           MinIO              MinIO
```

### Stock Market — Near Real-Time
Cotações coletadas a cada 15 minutos via Yahoo Finance.

```
Yahoo Finance → Airflow DAG (*/15 min) → PostgreSQL → Metabase
```

### Catalogação
Todas as tabelas registradas no Unity Catalog com metadados e localização dos arquivos.

```
Unity Catalog
└── portfolio
    ├── bronze   → tabelas brutas
    ├── silver   → tabelas limpas
    └── gold     → KPIs prontos para consumo
```

---

## Stack

| Camada | Tecnologia | Versão |
|---|---|---|
| Orquestração | Apache Airflow | 3.2.2 |
| Object Storage | MinIO (S3-compatible) | Latest |
| Processamento | DuckDB + Python | 1.5.4 / 3.11 |
| Banco de Dados | PostgreSQL | 16 |
| Catalogação | Unity Catalog Open Source | Latest |
| Visualização | Metabase | Latest |
| Infraestrutura | Docker Compose | — |

---

## Datasets

**E-Commerce**
Brazilian E-Commerce Public Dataset — [Olist (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
99.441 pedidos · 2016–2018 · Licença CC BY-NC-SA 4.0

**Stock Market**
Yahoo Finance via `yfinance` · Tickers: AAPL, MSFT, HSIC · Campos: price, open, high, low, volume · USD

---

## Resultados

| Pipeline | Métrica | Valor |
|---|---|---|
| E-Commerce | Pedidos processados | 99.441 |
| E-Commerce | Período coberto | 25 meses |
| E-Commerce | Média de entrega | 14,3 dias |
| E-Commerce | Tabelas Gold | vendas_mensais, performance_estados |
| Stock Market | Tickers monitorados | AAPL, MSFT, HSIC |
| Stock Market | Frequência de coleta | 15 minutos |

---

## Como Executar

**Pré-requisitos:** Docker, Python 3.11, 8 GB RAM

```bash
# 1. Clonar
git clone https://github.com/Henry-Schein-Brasil/data-projects-young-e2e.git
cd data-projects-young-e2e

# 2. Configurar variáveis de ambiente
cp .env.example docker/.env
# Edite docker/.env com suas chaves antes de continuar

# 3. Subir infraestrutura
cd docker && docker compose up -d

# 4. Criar ambiente Python
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

**Pipeline Stock Market (manual):**
```bash
python scripts/collect_stocks.py
```

**Testes de qualidade:**
```bash
python tests/test_pipeline.py
```

---

## Serviços

| Serviço | URL |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Unity Catalog | http://localhost:8081 |
| Unity Catalog UI | http://localhost:3001 |

Credenciais em `docker/.env` — não commitadas no repositório.

---

## DAGs

| DAG | Schedule | Descrição |
|---|---|---|
| `pipeline_olist` | Diário às 6h | Bronze → Silver → Gold |
| `collect_stocks_15min` | A cada 15 min | Yahoo Finance → PostgreSQL |

```bash
# Trigger manual
docker exec docker-airflow-scheduler-1 airflow dags trigger <dag_id>
```

---

## Estrutura

```
data-platform/
├── docker/
│   ├── dags/
│   │   ├── dag_pipeline_olist.py
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
│   └── test_pipeline.py
├── .env.example
└── requirements.txt
```

---

## Equivalência Corporativa

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

*Daniel Santos · [@danielbattdev](https://github.com/danielbattdev) · Henry Schein Brasil*
