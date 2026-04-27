# SQL-AVENGERS---BATCH-4
# 🛒 Retail Data Engineering & Analytics Pipeline

## 📌 Project Overview

This project demonstrates an **end-to-end data engineering pipeline** built using **Snowflake, SQL, and Power BI**.
It covers the complete lifecycle from **data ingestion → validation → transformation → analytics → visualization**.

The goal is to transform raw retail data into **actionable business insights**, including customer behavior, sales performance, and anomaly detection.

---

## 🏗️ Architecture Overview

```
RAW → VALIDATED → CURATED → DATAMART → ANALYTICS → POWER BI
```

### 🔹 Layers Explained

* **RAW Layer**

  * Data ingested from AWS S3 using Snowflake **Stages & Pipes**
  * Stores unprocessed data

* **VALIDATED Layer**

  * Data cleaning, deduplication, and validation
  * Error handling using **DQ exception logs**

* **CURATED Layer**

  * Star schema design with **Fact & Dimension tables**
  * Includes SCD Type 2 implementation

* **DATAMART Layer**

  * Business-friendly views for reporting

* **ANALYTICS Layer**

  * Customer 360
  * Segmentation
  * Churn analysis
  * Recommendation logic

---

## ⚙️ Tech Stack

* **Snowflake** (Data Warehouse)
* **SQL** (Transformations & Logic)
* **AWS S3** (Data Storage)
* **Power BI** (Visualization)
* **Streams & Tasks** (Automation)

---

## 📂 Data Model

### 🔹 Dimension Tables

* `dim_customers` (SCD Type 2)
* `dim_products`
* `dim_date`

### 🔹 Fact Table

* `fact_orders`

---

## 🔄 Data Pipeline

### ✅ 1. Data Ingestion

* Snowpipe loads data from S3 into RAW tables

### ✅ 2. Validation Layer

* Null checks
* Deduplication using `ROW_NUMBER`
* Referential integrity
* Error logging (`dq_exception_log`)

### ✅ 3. Curated Layer

* Fact & Dimension modeling
* Surrogate key usage
* SCD Type 2 implementation

### ✅ 4. Automation

* Snowflake **Tasks** used for orchestration
* Stream-based incremental loading

---

## 🚨 Anomaly Detection Engine

Implemented rule-based anomaly detection:

* High Order Value
* Abnormal Quantity
* High Customer Activity

Stored in:

```
governance.anomaly_log
```

---

## 📊 Data Marts

Created business-ready views:

* `customer_mart`
* `order_mart`
* `product_mart`
* `customer_activity_mart`
* `anomaly_mart`

---

## 🧠 Analytics Features

### 🔹 Customer 360

* Total orders, spending, last activity

### 🔹 Customer Segmentation

* High / Medium / Low value customers

### 🔹 Sales Insights

* Product performance & revenue trends

### 🔹 Churn Detection

* Customers inactive for 30+ days

### 🔹 Recommendation Engine

* Suggests top-selling products not yet purchased

---

## 📈 Power BI Dashboard

### Key KPIs:

* Total Revenue
* Total Orders
* Total Customers
* Avg Order Value

### Visualizations:

* Revenue Trend (Line Chart)
* Customer Segmentation (Donut Chart)
* Product Performance (Bar Chart)
* Anomaly Monitoring (Table + KPI)

---

## 🛠️ Key Features

* End-to-end data pipeline
* Incremental processing using Streams
* Automated workflows using Tasks
* Data quality handling & error logging
* Star schema design
* Business-ready analytics layer

---

## 🚀 How to Run

1. Load data into RAW layer via S3
2. Run validation procedure:

   ```
   CALL validated.process_retail_data_quality();
   ```
3. Load curated layer:

   ```
   CALL curated.sp_load_curated_layer();
   ```
4. Run anomaly detection:

   ```
   CALL analytics.sp_detect_anomalies();
   ```
5. Connect Power BI to Snowflake

---

## 🎯 Business Value

* Enables **data-driven decision making**
* Identifies **high-value customers**
* Detects **fraud/anomalies**
* Improves **customer retention strategies**

---

## 🏆 Conclusion

This project demonstrates a **production-ready data pipeline** with:

✔ Data Engineering
✔ Data Modeling
✔ Analytics
✔ Visualization

---

## 👤 Author

**Madhavan Dodda**

---

## ⭐ If you like this project, give it a star!
