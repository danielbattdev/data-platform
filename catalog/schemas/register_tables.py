import requests
import os
from dotenv import load_dotenv

load_dotenv(os.path.expanduser('~/data-platform/.env'))

UC_URL = os.getenv('UC_BASE_URL', 'http://localhost:8081')
HEADERS = {'Content-Type': 'application/json'}

def create_catalog(name, comment=""):
    r = requests.post(f"{UC_URL}/api/2.1/unity-catalog/catalogs",
                      headers=HEADERS, json={"name": name, "comment": comment})
    if r.status_code in [200, 409]:
        print(f"✅ Catalog '{name}' OK.")
    else:
        print(f"❌ Catalog '{name}': {r.status_code} - {r.text}")

def create_schema(catalog, name, comment=""):
    r = requests.post(f"{UC_URL}/api/2.1/unity-catalog/schemas",
                      headers=HEADERS,
                      json={"name": name, "catalog_name": catalog, "comment": comment})
    if r.status_code in [200, 409]:
        print(f"✅ Schema '{catalog}.{name}' OK.")
    else:
        print(f"❌ Schema '{catalog}.{name}': {r.status_code} - {r.text}")

def register_table(catalog, schema, name, location, comment=""):
    r = requests.post(f"{UC_URL}/api/2.1/unity-catalog/tables",
                      headers=HEADERS,
                      json={"name": name, "catalog_name": catalog,
                            "schema_name": schema, "table_type": "EXTERNAL",
                            "data_source_format": "PARQUET",
                            "storage_location": location, "comment": comment})
    if r.status_code in [200, 409]:
        print(f"✅ Tabela '{catalog}.{schema}.{name}' → {location}")
    else:
        print(f"❌ Tabela '{name}': {r.status_code} - {r.text}")

if __name__ == '__main__':
    print("\n--- Criando catálogo ---")
    create_catalog("portfolio", "Catálogo principal do projeto portfólio")

    print("\n--- Criando schemas ---")
    create_schema("portfolio", "bronze", "Dados brutos — imutáveis. Fonte original Kaggle.")
    create_schema("portfolio", "silver", "Dados limpos, tipados, sem nulos críticos.")
    create_schema("portfolio", "gold",   "Métricas de negócio prontas para consumo.")

    print("\n--- Registrando tabelas ---")
    register_table("portfolio", "bronze", "orders_raw",
                   "s3://bronze/olist/orders/",
                   "Pedidos brutos Olist — sem transformação")
    register_table("portfolio", "silver", "orders_clean",
                   "s3://silver/olist/orders/",
                   "Pedidos limpos com métricas de entrega")
    register_table("portfolio", "gold", "vendas_mensais",
                   "s3://gold/olist/vendas_mensais/",
                   "KPIs mensais de vendas")
    register_table("portfolio", "gold", "performance_estados",
                   "s3://gold/olist/performance_estados/",
                   "Performance de entrega por estado")

    print("\n🎉 Catálogo registrado com sucesso!")
