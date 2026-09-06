<div align="center">

# 🛒 Ecommerce Batch ETL Pipeline

**An end-to-end batch data pipeline for synthetic e-commerce data** — from generation to analytics.

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white)
![MariaDB](https://img.shields.io/badge/MariaDB-Source%20DB-003545?style=flat-square&logo=mariadb&logoColor=white)
![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-1.14.0-728E9B?style=flat-square&logo=apache&logoColor=white)
![HDFS](https://img.shields.io/badge/HDFS-Raw%20Layer-66CCFF?style=flat-square&logo=apachehadoop&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Apache Hive](https://img.shields.io/badge/Apache%20Hive-3.1.2-FDEE21?style=flat-square&logo=apachehive&logoColor=black)
![Status](https://img.shields.io/badge/Status-Complete-success?style=flat-square)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Data Volumes](#-data-volumes)
- [Pipeline Stages](#-pipeline-stages)
- [How to Run](#-how-to-run)
- [Requirements](#-requirements)
- [Validation](#-validation)
- [Deliverables](#-deliverables)
- [Author](#-author)

---

## 🔎 Overview

This project implements a complete **batch ETL pipeline** for synthetic e-commerce data.
Data is generated in Python, persisted in **MariaDB**, extracted with **Apache NiFi** into
an **HDFS** raw layer, cleaned and transformed with **Apache Spark**, and finally exposed
as analytics-ready tables in **Apache Hive** for querying.

---

## 🏗️ Architecture

```
┌───────────────────┐     ┌───────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────────┐     ┌────────────────┐
│  Python Generator  │ ──▶ │  MariaDB  │ ──▶ │ Apache NiFi  │ ──▶ │    HDFS    │ ──▶ │ Apache Spark │ ──▶ │  Apache Hive   │
│  (Faker + batch    │     │  (Source  │     │ (Extraction) │     │ (Raw Layer)│     │(Transform &  │     │ (Analytics DW) │
│    inserts)        │     │  tables)  │     │              │     │            │     │  Clean)      │     │                │
└───────────────────┘     └───────────┘     └──────────────┘     └────────────┘     └──────────────┘     └────────────────┘
```

---

## 📁 Project Structure

```
ecommerce_etl_project/
├── 01_data_generation/
│   └── generate_data.py                  # Synthetic data generation + batch load into MariaDB
├── 02_mariadb/
│   └── create_tables.sql                 # Database & table DDL for the MariaDB source layer
├── 03_nifi/
│   └── nifi_mariadb_to_hdfs_flow.xml     # Exported NiFi flow definition (extraction)
├── 04_hdfs/
│   └── hdfs_raw_directory_structure.txt  # Listing of the HDFS raw/staging zone
├── 05_spark/
│   └── transform.py                      # PySpark cleaning + business transformation script
├── 06_hive/
│   └── create_hive_tables.hql            # Hive database & external table DDL
├── 07_validation/
│   ├── validation_mariadb.sql            # Source-layer record count & integrity checks
│   ├── validation_hive.sql               # Analytics-layer data quality & reconciliation checks
│   └── run_validation.sh                 # Runs both validations, saves clean output
└── README.md
```

---

## 📊 Data Volumes

| Table | Records |
|:--|--:|
| `customers` | 100,000 |
| `products` | 20,000 |
| `orders` | 500,000 |
| `order_items` | 2,000,000 |
| `payments` | 500,000 |

---

## ⚙️ Pipeline Stages

| Stage | Tool | Description |
|:--|:--|:--|
| **1. Data Generation** | Python (Faker) | Synthetic e-commerce data, inserted into MariaDB in batches of 5,000 rows |
| **2. Source Storage** | MariaDB | Database `ecommerce` with 5 source tables |
| **3. Extraction** | Apache NiFi | `ExecuteSQL → ConvertRecord (Avro → CSV) → PutHDFS` per table |
| **4. Raw Layer** | HDFS | Untransformed CSV data at `/staging_zone/ecommerce/<table>` |
| **5. Transformation** | Apache Spark | Dedup, null handling, type casting, date/price/quantity validation, business joins |
| **6. Data Warehouse** | Apache Hive | Database `ecommerce_dw` with external Parquet-backed tables |
| **7. Validation** | SQL / HQL | Record-count and reconciliation checks |

**Analytics tables produced:** `fact_sales` · `daily_sales` · `customer_sales` · `product_sales` · `monthly_sales` · `yearly_sales` · `sales_by_country` · `sales_by_category`

---

## 🚀 How to Run

```bash
# 1. Generate data
cd 01_data_generation && python3 generate_data.py

# 2. Build the NiFi flow manually in the NiFi UI (see 03_nifi/) and start it

# 3. Verify raw data landed in HDFS
hdfs dfs -ls -R /staging_zone/ecommerce/

# 4. Run the Spark transformation
cd ../05_spark && spark-submit --master local[*] transform.py

# 5. Create the Hive database and tables
cd ../06_hive && hive -f create_hive_tables.hql

# 6. Run validation
cd ../07_validation && bash run_validation.sh
```

---

## 🧰 Requirements

| Component | Details |
|:--|:--|
| Python 3 | `mysql-connector-python`, `faker` |
| MariaDB | Source relational database |
| Apache NiFi | v1.14.0 + `mariadb-java-client-3.5.10.jar` on the classpath |
| Hadoop / HDFS | Distributed storage for raw & analytics layers |
| Apache Spark | PySpark for transformation |
| Apache Hive | v3.1.2 for the analytics warehouse |

---

## ✅ Validation

Validation is split into two independent scripts to match each layer:

- **`validation_mariadb.sql`** — confirms source row counts and referential integrity (zero orphaned orders).
- **`validation_hive.sql`** — checks null values, invalid prices/quantities, duplicate keys, future dates, and reconciles totals across `fact_sales`, `daily_sales`, `customer_sales`, and `product_sales`.

Run both at once and capture clean output with:
```bash
bash 07_validation/run_validation.sh
```

---

## 📦 Deliverables

- [x] Python data-generation script
- [x] MariaDB database & table creation SQL
- [x] NiFi flow/template
- [x] HDFS raw-data directory structure
- [x] PySpark transformation script
- [x] Hive database & table creation scripts
- [x] Data-quality & record-count validation queries

---

## 👤 Author

**Yusuf Shoman**
📧 yusufshoman@gmail.com

---

<div align="center">
<sub>Built as a hands-on Data Engineering project covering the full batch ETL lifecycle.</sub>
</div>
