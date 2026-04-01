# theLook End-to-End Data Pipeline

This project demonstrates a complete end-to-end data pipeline for educational purposes in Data Engineering.

## Architecture

```
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Google BigQuery   │────▶│   mkpipe (ETL)   │────▶│   ClickHouse    │
│ (thelook-ecommerce) │     │                  │     │     (DWH)       │
└─────────────────────┘     └──────────────────┘     └─────────────────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────────┐
                                                         │      dbt        │
                                                         │ (Transformation)│
                                                         └─────────────────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────────┐
                                                         │   Dagster       │
                                                         │ (Orchestration) │
                                                         └─────────────────┘
                                                                 │
                                    ┌──────────────────────────────┘
                                    ▼
                        ┌─────────────────────┐
                        │   PostgreSQL        │
                        │  (Application Logs) │
                        └─────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-------------|
| Source | Google BigQuery (theLook Ecommerce) |
| DWH | ClickHouse (Docker) |
| Logs | PostgreSQL |
| ETL | mkpipe (custom) |
| Transformation | dbt |
| Orchestration | Dagster |
| Deployment | AWS EC2 (Docker) |

## Project Structure

```
theLook/
├── dagster_project/           # Dagster definitions
│   ├── assets/               # Dagster assets
│   │   ├── ingestion.py      # Data ingestion assets
│   │   ├── dbt_assets.py     # dbt model assets
│   │   └── distribution.py   # Distribution assets
│   ├── config/               # Configuration
│   │   ├── settings.py       # Settings
│   │   └── mkpipe_parser.py # mkpipe parser
│   ├── resources/            # Dagster resources
│   │   └── mkpipe_resource.py
│   ├── jobs.py              # Job definitions
│   ├── schedules.py         # Schedule definitions
│   └── definitions.py       # Dagster definitions
│
├── dbt_project/             # dbt project
│   ├── models/
│   │   ├── stg/             # Staging models
│   │   │   ├── stg_users.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_inventory_items.sql
│   │   │   ├── stg_events.sql
│   │   │   └── stg_distribution_centers.sql
│   │   └── mart/            # Mart models
│   │       ├── mart_aov_daily.sql
│   │       ├── mart_category_sales.sql
│   │       └── mart_customer_revenue.sql
│   ├── macros/              # dbt macros
│   ├── snapshots/           # dbt snapshots
│   └── tests/               # dbt tests
│
├── extract_load_project/    # mkpipe ETL project
│   └── mkpipe_project.yaml # mkpipe configuration
│
├── deployment/              # AWS EC2 deployment
│   ├── init-webserver.sh   # Web server init script
│   ├── init-daemon.sh      # Daemon init script
│   └── init-postgres.sql   # PostgreSQL schema
│
├── docker-compose.yaml     # Docker composition
├── Dockerfile               # Docker image
└── .env                     # Environment variables
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- GCP Service Account with BigQuery access
- AWS EC2 instance (optional for deployment)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd theLook
```

2. Copy and configure credentials:
```bash
cp .credentials.yaml.example .credentials.yaml
# Edit .credentials.yaml with your GCP credentials
```

3. Start infrastructure:
```bash
docker-compose up -d
```

4. Run the pipeline:
```bash
docker-compose run --rm pipeline
```

## Pipeline Flow

1. **Ingestion (mkpipe)**: Extract data from BigQuery → Load to ClickHouse
2. **Transformation (dbt)**: Transform staging data → Create mart tables
3. **Orchestration (Dagster)**: Schedule and monitor pipeline runs
4. **Logging**: Application logs → PostgreSQL

## Documentation

For detailed architecture documentation, visit: https://m-karakus.github.io/docs/architecture/data-architecture

---

# theLook Uçtan Uca (End-to-End) Veri Hattı

Bu proje, Veri Mühendisliği eğitimi için tasarlanmış komple bir uçtan uca veri hattı (data pipeline) demo projesidir.

## Mimari

```
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Google BigQuery  │────▶│   mkpipe (ETL)   │────▶│   ClickHouse    │
│ (thelook-ecommerce) │     │                  │     │     (DWH)       │
└─────────────────────┘     └──────────────────┘     └─────────────────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────────┐
                                                         │      dbt        │
                                                         │ (Dönüştürme)    │
                                                         └─────────────────┘
                                                                 │
                                                                 ▼
                                                         ┌─────────────────┐
                                                         │   Dagster       │
                                                         │ ( Orkestrasyon) │
                                                         └─────────────────┘
                                                                 │
                                    ┌──────────────────────────────┘
                                    ▼
                        ┌─────────────────────┐
                        │   PostgreSQL        │
                        │  (Uygulama Logları) │
                        └─────────────────────┘
```

## Teknoloji Stack

| Bileşen | Teknoloji |
|---------|-----------|
| Kaynak | Google BigQuery (theLook Ecommerce) |
| DWH | ClickHouse (Docker) |
| Loglar | PostgreSQL |
| ETL | mkpipe (özel) |
| Dönüştürme | dbt |
| Orkestrasyon | Dagster |
| Deployment | AWS EC2 (Docker) |

## Proje Yapısı

```
theLook/
├── dagster_project/           # Dagster tanımları
│   ├── assets/               # Dagster varlıkları
│   │   ├── ingestion.py      # Veri alma varlıkları
│   │   ├── dbt_assets.py    # dbt model varlıkları
│   │   └── distribution.py   # Dağıtım varlıkları
│   ├── config/               # Konfigürasyon
│   │   ├── settings.py       # Ayarlar
│   │   └── mkpipe_parser.py # mkpipe ayrıştırıcı
│   ├── resources/            # Dagster kaynakları
│   │   └── mkpipe_resource.py
│   ├── jobs.py              # İş tanımları
│   ├── schedules.py         # Zamanlama tanımları
│   └── definitions.py       # Dagster tanımları
│
├── dbt_project/             # dbt projesi
│   ├── models/
│   │   ├── stg/             # Stage modelleri
│   │   │   ├── stg_users.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_inventory_items.sql
│   │   │   ├── stg_events.sql
│   │   │   └── stg_distribution_centers.sql
│   │   └── mart/            # Mart modelleri
│   │       ├── mart_aov_daily.sql
│   │       ├── mart_category_sales.sql
│   │       └── mart_customer_revenue.sql
│   ├── macros/              # dbt makroları
│   ├── snapshots/           # dbt snapshotları
│   └── tests/               # dbt testleri
│
├── extract_load_project/    # mkpipe ETL projesi
│   └── mkpipe_project.yaml # mkpipe konfigürasyonu
│
├── deployment/              # AWS EC2 deployment
│   ├── init-webserver.sh   # Web sunucu init scripti
│   ├── init-daemon.sh      # Daemon init scripti
│   └── init-postgres.sql   # PostgreSQL şeması
│
├── docker-compose.yaml     # Docker composition
├── Dockerfile               # Docker image
└── .env                     # Environment değişkenleri
```

## Başlangıç

### Gereksinimler

- Docker & Docker Compose
- Python 3.11+
- BigQuery erişimli GCP Servis Hesabı
- AWS EC2 instance (deployment için opsiyonel)

### Kurulum

1. Repoyu klonlayın:
```bash
git clone <repository-url>
cd theLook
```

2. Credential dosyasını kopyalayın ve düzenleyin:
```bash
cp .credentials.yaml.example .credentials.yaml
# .credentials.yaml dosyasını GCP credentiallarınızla düzenleyin
```

3. Altyapıyı başlatın:
```bash
docker-compose up -d
```

4. Pipeline'ı çalıştırın:
```bash
docker-compose run --rm pipeline
```

## Pipeline Akışı

1. **Veri Alma (mkpipe)**: BigQuery'den veriyi çek → ClickHouse'a yükle
2. **Dönüştürme (dbt)**: Stage veriyi dönüştür → Mart tabloları oluştur
3. **Orkestrasyon (Dagster)**: Pipeline çalışmalarını zamanla ve izle
4. **Loglama**: Uygulama logları → PostgreSQL

## Detaylı Dokümantasyon

Detaylı mimari dokümantasyonu için: https://m-karakus.github.io/docs/architecture/data-architecture
