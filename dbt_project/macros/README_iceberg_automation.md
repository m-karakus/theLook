# Iceberg Table Automation

## Overview

Bu macro'lar mkpipe tarafından oluşturulan Iceberg tablolarını Snowflake'e otomatik olarak tanıtır ve refresh eder.

## Problem

1. **mkpipe** → S3/Glue'ya Iceberg tabloları yazar (örn: `raw_dce__bill_cycle_def`)
2. **Snowflake** → Bu tabloları görmek için manuel `CREATE ICEBERG TABLE` gerekiyor
3. **dbt** → mkpipe çalıştıktan sonra, dbt başlamadan önce `ALTER ICEBERG TABLE REFRESH` gerekiyor

## Solution

3 macro ile otomatik çözüm:

### 1. `get_raw_iceberg_tables()`
- `_raw__sources.yml`'den `raw_*` ile başlayan tüm tabloları çıkarır
- Dinamik liste, yeni tablo eklendiğinde otomatik güncellenir

### 2. `create_iceberg_tables()`
- Her `dbt run` başlangıcında çalışır
- Tablo yoksa oluşturur (idempotent)
- Varsa skip eder

### 3. `refresh_iceberg_tables()`
- Her `dbt run` başlangıcında çalışır
- Tüm Iceberg tablolarını refresh eder
- mkpipe'ın yazdığı son veriyi Snowflake'e senkronize eder

## Workflow

```bash
# 1. mkpipe veriyi S3/Glue'ya yazar
cd extract_load_project
mkpipe run --config mkpipe_project_local.yaml --pipeline dce_to_dwh

# 2. dbt çalıştırıldığında otomatik olarak:
cd ../dbt_project
dbt run --select tag:api
# → on-run-start: create_iceberg_tables() (yoksa oluştur)
# → on-run-start: refresh_iceberg_tables() (metadatayı güncelle)
# → dbt models çalışır (fresh data ile)
```

## Yeni Raw Tablo Ekleme

1. mkpipe'a yeni tablo ekle (`mkpipe_project_local.yaml`)
2. `_raw__sources.yml`'ye ekle:
   ```yaml
   - name: raw_dce__yeni_tablo
   ```
3. **Hiçbir şey yapma!** dbt bir sonraki çalıştığında otomatik oluşturulur

## Manual Test

```sql
-- Tek bir tablo için test
SELECT * FROM DWH_STG.raw_dce__bill_cycle_def LIMIT 10;

-- Tüm raw tabloları listele
SHOW ICEBERG TABLES IN SCHEMA DWH_STG;

-- Manuel refresh (gerekirse)
ALTER ICEBERG TABLE DWH_STG.raw_dce__bill_cycle_def REFRESH;
```

## Configuration

`dbt_project.yml`:
```yaml
vars:
  raw_schema: "DWH_STG"  # Iceberg tablolarının bulunduğu şema
  raw_incremental_days: 3

on-run-start:
  - "{{ create_iceberg_tables() }}"
  - "{{ refresh_iceberg_tables() }}"

models:
  dbt_project:
    dwh_stg:
      +schema: dwh_stg
      +tags: ['layer:stg']  # Tag ile filtreleme için
```

Schema override (gerekirse):
```bash
dbt run --vars '{"raw_schema": "CUSTOM_SCHEMA"}'
```

## Snowflake Prerequisites

```sql
-- External volume (bir kere kurulum)
CREATE EXTERNAL VOLUME iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_iceberg'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://darwin-msint-snowflake/data_lake/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::...'
    )
  );

-- Glue catalog integration (bir kere kurulum)
CREATE CATALOG INTEGRATION glue_catalog_integration
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'iceberg_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::...'
  GLUE_CATALOG_ID = '...'
  GLUE_REGION = 'ca-central-1'
  ENABLED = TRUE;
```

## Troubleshooting

### Tablo oluşturulmuyor
```bash
dbt run-operation create_iceberg_tables
```

### Refresh çalışmıyor
```bash
dbt run-operation refresh_iceberg_tables
```

### Logs
```bash
# dbt run çıktısında göreceksin:
# === Checking Iceberg Tables ===
# Found 20 raw tables in sources.yml
# ✓ Created Iceberg table: DWH_STG.raw_dce__bill_cycle_def
# === Iceberg Table Check Complete ===
# === Refreshing Iceberg Tables ===
# ✓ Refreshed: raw_dce__bill_cycle_def
# === Iceberg Refresh Complete ===
```
