# Data Platform — Henry Schein Brasil

Plataforma de dados local desenvolvida como projeto de portfólio, demonstrando capacidade técnica em Engenharia de Dados com ferramentas open source e arquitetura de mercado.

**Desenvolvido por:** Daniel Santos
**Período:** Junho 2026

---

## Visão Geral

Implementação completa de uma plataforma de dados moderna com dois pipelines independentes:

- **Pipeline E-Commerce** — Arquitetura Medallion (Bronze → Silver → Gold) com dados reais da Olist
- **Pipeline Financeiro** — Coleta automática de cotações a cada 15 minutos via Yahoo Finance

---

## Stack Tecnológica

| Componente | Tecnologia | Versão | Função |
|------------|-----------|--------|--------|
| Object Storage | MinIO (S3-compatible) | Latest | Armazenamento Bronze/Silver/Gold em Parquet |
| Processamento | DuckDB + Python | 1.5.4 / 3.11 | Transformações analíticas |
| Orquestração | Apache Airflow | 3.2.2 | Agendamento e execução dos pipelines |
| Catalogação | Unity Catalog OS | Latest | Governança e descoberta de dados |
| Visualização | Metabase | Latest | Dashboards e KPIs de negócio |
| Banco de Dados | PostgreSQL | 16 | Metadata store, camada Gold e dados financeiros |
| Infraestrutura | Docker Compose | - | Containerização de todos os serviços |

---

## Arquitetura

### Pipeline 1 — E-Commerce Olist (Batch Diário)

\`\`\`
Kaggle (Fonte)
      ↓
  Bronze Layer  → Dados brutos em Parquet — imutáveis       (MinIO: s3://bronze)
      ↓
  Silver Layer  → Dados limpos, tipados, sem nulos críticos  (MinIO: s3://silver)
      ↓
  Gold Layer    → KPIs e métricas de negócio                 (MinIO: s3://gold)
      ↓
  PostgreSQL + Metabase → Dashboard E-Commerce
\`\`\`

### Pipeline 2 — Stock Market (A cada 15 minutos)

\`\`\`
Yahoo Finance API
      ↓
  Airflow DAG → Coleta AAPL, MSFT, HSIC
      ↓
  PostgreSQL → Tabela stock_prices
      ↓
  Metabase → Dashboard Stock Market Monitor
\`\`\`

### Catalogação

\`\`\`
Unity Catalog
  └── portfolio (catálogo)
      ├── bronze  → tabelas brutas registradas
      ├── silver  → tabelas limpas registradas
      └── gold    → KPIs com metadados
\`\`\`

---

## Datasets

**Pipeline E-Commerce:**
- Brazilian E-Commerce — Olist (Kaggle)
- 99.441 pedidos reais (2016–2018)
- 9 arquivos CSV: pedidos, clientes, produtos, vendedores, pagamentos
- Licença: CC BY-NC-SA 4.0

**Pipeline Financeiro:**
- Fonte: Yahoo Finance (yfinance)
- Tickers: AAPL (Apple), MSFT (Microsoft), HSIC (Henry Schein)
- Campos: price, open, high, low, volume
- Frequência: a cada 15 minutos — Moeda: USD

---

## Resultados

### E-Commerce

| Métrica | Valor |
|---------|-------|
| Total de pedidos processados | 99.441 |
| Período coberto | 25 meses |
| Média de dias para entrega | 14,3 dias |
| Camadas implementadas | Bronze, Silver, Gold |
| Tabelas Gold | vendas_mensais, performance_estados |

### Stock Market

| Métrica | Valor |
|---------|-------|
| Tickers monitorados | AAPL, MSFT, HSIC |
| Frequência de coleta | 15 minutos |
| Campos coletados | price, open, high, low, volume |
| Armazenamento | PostgreSQL — tabela stock_prices |

---

## Estrutura do Projeto

\`\`\`
data-platform/
├── airflow/dags/processing/
│   ├── dag_pipeline_olist.py      ← Pipeline E-Commerce (diário 6h)
│   └── dag_collect_stocks.py      ← Pipeline Financeiro (15 min)
├── duckdb/scripts/
│   ├── ingest.py                  ← CSV → Bronze Parquet
│   ├── transform_silver.py        ← Bronze → Silver
│   └── transform_gold.py          ← Silver → Gold
├── catalog/schemas/
│   └── register_tables.py         ← Registro no Unity Catalog
├── scripts/
│   └── collect_stocks.py          ← Coleta Yahoo Finance → PostgreSQL
├── tests/
│   └── test_pipeline.py           ← Testes de qualidade
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
\`\`\`

---

## Deploy — Como Executar

### Pré-requisitos
- Docker + Docker Compose
- Python 3.11
- 8GB RAM mínimo

\`\`\`bash
# 1. Clonar o repositório
git clone https://github.com/Henry-Schein-Brasil/data-projects-young-e2e.git
cd data-projects-young-e2e

# 2. Configurar variáveis de ambiente
cp .env.example .env

# 3. Subir toda a infraestrutura
docker compose up -d

# 4. Criar virtualenv Python 3.11
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 5. Executar pipeline E-Commerce
python duckdb/scripts/ingest.py
python duckdb/scripts/transform_silver.py
python duckdb/scripts/transform_gold.py

# 6. Executar coleta de stocks (manual)
python scripts/collect_stocks.py

# 7. Executar testes de qualidade
python tests/test_pipeline.py
\`\`\`

### Serviços Disponíveis

| Serviço | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Unity Catalog | http://localhost:8081 |
| PostgreSQL | localhost:5432 |

---

## DAGs Airflow

| DAG | Schedule | Função |
|-----|----------|--------|
| `pipeline_olist` | Diário às 6h | Bronze → Silver → Gold |
| `collect_stocks_15min` | A cada 15 min | Yahoo Finance → PostgreSQL |

\`\`\`bash
# Trigger manual E-Commerce
docker exec docker-airflow-scheduler-1 airflow dags trigger pipeline_olist

# Trigger manual Financeiro
docker exec docker-airflow-scheduler-1 airflow dags trigger collect_stocks_15min
\`\`\`

---

## Testes de Qualidade

\`\`\`bash
python tests/test_pipeline.py
# ✅ Bronze OK
# ✅ Silver OK
# ✅ Gold vendas_mensais OK
# ✅ Gold performance_estados OK
# 🎉 Todos os testes passaram!
\`\`\`

---

## Equivalência Corporativa

| Este Projeto | Ambiente Corporativo |
|---|---|
| MinIO | AWS S3 / Azure ADLS Gen2 |
| DuckDB | Apache Spark / Trino |
| Airflow local | Cloud Composer / MWAA |
| Metabase CE | Power BI / Tableau |
| Unity Catalog OS | Unity Catalog Enterprise |
| yfinance | Bloomberg API / Refinitiv |
| PostgreSQL local | AWS RDS / Azure SQL |

---

## Autor

**Daniel Santos**
GitHub: [@danielbattdev](https://github.com/danielbattdev)
Henry Schein Brasil — 2026
