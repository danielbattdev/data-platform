# Data Platform — Henry Schein Brasil

Plataforma de dados local desenvolvida como projeto de portfólio, demonstrando capacidade técnica em Engenharia de Dados com ferramentas open source e arquitetura de mercado.

**Desenvolvido por:** Daniel Santos
**Período:** Junho 2026  

---

## Visão Geral

Implementação completa de uma plataforma de dados moderna seguindo a **Arquitetura Medallion** (Bronze → Silver → Gold), utilizando exclusivamente ferramentas open source que espelham o ambiente corporativo de grandes empresas de dados.

---

## Stack Tecnológica

| Componente | Tecnologia | Versão | Função |
|------------|-----------|--------|--------|
| Object Storage | MinIO (S3-compatible) | Latest | Armazenamento das camadas Bronze/Silver/Gold |
| Processamento | DuckDB + Python | 1.5.4 / 3.11 | Transformações analíticas em Parquet |
| Orquestração | Apache Airflow | 3.2.2 | Agendamento e execução do pipeline |
| Catalogação | Unity Catalog OS | Latest | Governança e descoberta de dados |
| Visualização | Metabase | Latest | Dashboards e KPIs de negócio |
| Banco de Dados | PostgreSQL | 16 | Metadata store e camada Gold |
| Infraestrutura | Docker Compose | - | Containerização de todos os serviços |

---

## Arquitetura Medallion
Kaggle (Fonte)

↓

Bronze Layer          → Dados brutos em Parquet — imutáveis

(MinIO: s3://bronze)

↓

Silver Layer          → Dados limpos, tipados, sem nulos críticos

(MinIO: s3://silver)

↓

Gold Layer            → KPIs e métricas de negócio prontas para consumo

(MinIO: s3://gold)

↓

Metabase Dashboard    → Visualizações interativas
---

## Dataset

**Brazilian E-Commerce — Olist** (Kaggle)
- 99.441 pedidos reais (2016–2018)
- 9 arquivos CSV cobrindo pedidos, clientes, produtos, vendedores e pagamentos
- Licença: CC BY-NC-SA 4.0

---

## Resultados do Pipeline

| Métrica | Valor |
|---------|-------|
| Total de pedidos processados | 99.441 |
| Período coberto | 25 meses |
| Média de dias para entrega | 14,3 dias |
| Camadas implementadas | Bronze, Silver, Gold |
| Tabelas Gold disponíveis | vendas_mensais, performance_estados |

---

## Estrutura do Projeto
data-platform/

├── airflow/

│   └── dags/

│       └── processing/

│           └── dag_pipeline_olist.py    ← DAG principal do pipeline

├── duckdb/

│   └── scripts/

│       ├── ingest.py                    ← CSV → Bronze Parquet

│       ├── transform_silver.py          ← Bronze → Silver

│       └── transform_gold.py            ← Silver → Gold

├── catalog/

│   └── schemas/

│       └── register_tables.py           ← Registro no Unity Catalog

├── storage/

│   └── bronze/olist/                    ← Parquet files Bronze

├── tests/

│   └── test_pipeline.py                 ← Testes de qualidade

├── docker-compose.yml

├── requirements.txt

└── README.md
---

## Como Executar

### Pré-requisitos
- Docker + Docker Compose
- Python 3.11
- 8GB RAM mínimo

### Passo a Passo

```bash
# 1. Clonar o repositório
git clone https://github.com/Henry-Schein-Brasil/data-projects-young-e2e.git
cd data-projects-young-e2e

# 2. Configurar variáveis de ambiente
cp .env.example .env

# 3. Subir infraestrutura
docker compose up -d

# 4. Ativar ambiente Python
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 5. Executar pipeline
python duckdb/scripts/ingest.py
python duckdb/scripts/transform_silver.py
python duckdb/scripts/transform_gold.py

# 6. Executar testes
python tests/test_pipeline.py
```

### Serviços Disponíveis

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Metabase | http://localhost:3000 | — |
| Unity Catalog | http://localhost:8081 | — |

---

## Pipeline Airflow

A DAG `pipeline_olist` executa automaticamente todo dia às 6h:
ingestao_bronze → bronze_to_silver → silver_to_gold
Trigger manual:
```bash
docker exec docker-airflow-scheduler-1 airflow dags trigger pipeline_olist
```

---

## Testes de Qualidade

```bash
python tests/test_pipeline.py

# Output esperado:
# ✅ Bronze OK
# ✅ Silver OK
# ✅ Gold vendas_mensais OK
# ✅ Gold performance_estados OK
# 🎉 Todos os testes passaram!
```

---

## Equivalência Corporativa

| Este Projeto | Ambiente Corporativo |
|---|---|
| MinIO | AWS S3 / Azure ADLS Gen2 |
| DuckDB | Apache Spark / Trino |
| Airflow local | Cloud Composer / MWAA |
| Metabase CE | Power BI / Tableau |
| Unity Catalog OS | Unity Catalog Enterprise |

---

## Autor

**Daniel Santos**  
GitHub: [@danielbattdev](https://github.com/danielbattdev)  
Henry Schein Brasil — 2026
