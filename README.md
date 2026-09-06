# Ecommerce Batch ETL Pipeline

End-to-end batch data pipeline for synthetic e-commerce data, covering generation, storage,
extraction, transformation, and analytics.

## Architecture

```
Python Generator → MariaDB → Apache NiFi → HDFS (Raw Layer) → Apache Spark → Apache Hive (Analytics)
```

## Project Structure

```
ecommerce_etl_project/
├── 01_data_generation/
│   └── generate_data.py          # Generates synthetic data and loads it into MariaDB (batch inserts)
├── 02_mariadb/
│   └── create_tables.sql         # Database & table DDL for the MariaDB source layer
├── 03_nifi/
│   └── nifi_mariadb_to_hdfs_flow.xml   # Exported NiFi flow definition (extraction)
├── 04_hdfs/
│   └── hdfs_raw_directory_structure.txt # Listing of the HDFS raw/staging zone
├── 05_spark/
│   └── transform.py              # PySpark cleaning + business transformation script
├── 06_hive/
│   └── create_hive_tables.hql    # Hive database & external table DDL
├── 07_validation/
│   ├── validation_mariadb.sql    # Source-layer record count & integrity checks
│   ├── validation_hive.sql       # Analytics-layer data quality & reconciliation checks
│   └── run_validation.sh         # Runs both validations and saves clean output
└── README.md
```

## Data Volumes

| Table | Records |
|---|---|
| customers | 100,000 |
| products | 20,000 |
| orders | 500,000 |
| order_items | 2,000,000 |
| payments | 500,000 |

## Pipeline Stages

1. **Data Generation (Python)** — Synthetic e-commerce data generated with Faker, inserted
   into MariaDB using batch inserts (5,000 rows/batch).
2. **Source Storage (MariaDB)** — Database `ecommerce` with 5 source tables.
3. **Extraction (Apache NiFi)** — `ExecuteSQL → ConvertRecord (Avro → CSV) → PutHDFS` flow
   per table, writing raw data to `/staging_zone/ecommerce/<table>`.
4. **Raw Layer (HDFS)** — Untransformed CSV data preserved per source table.
5. **Transformation (Apache Spark)** — Cleans (dedup, nulls, types, date/price/quantity
   validation) and joins `orders + order_items + products + customers`, producing
   `fact_sales`, `daily_sales`, `customer_sales`, `product_sales`, plus rollups
   (`monthly_sales`, `yearly_sales`, `sales_by_country`, `sales_by_category`) as Parquet
   under `/analytics_zone/ecommerce/`.
6. **Data Warehouse (Apache Hive)** — Database `ecommerce_dw` with external tables over the
   Parquet analytics layer.
7. **Validation** — Record-count and reconciliation checks across MariaDB and Hive.

## How to Run

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

## Requirements

- Python 3, `mysql-connector-python`, `faker`
- MariaDB
- Apache NiFi 1.14.0 (with `mariadb-java-client-3.5.10.jar` on the classpath)
- Hadoop / HDFS
- Apache Spark (PySpark)
- Apache Hive 3.1.2
