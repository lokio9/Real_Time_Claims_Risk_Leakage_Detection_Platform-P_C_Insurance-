# 📌 Overview

This project is a complete end-to-end real-time claims risk and leakage detection platform for Property & Casualty (P&C) insurance companies.

**Key Capabilities:**
- Detect suspicious claims early (FNOL stage)
- Identify financial leakage in claim settlements
- Track risk exposure in real-time
- Automate data ingestion pipelines
- Serve insights through APIs and dashboards

> This is not just a data pipeline — it is a full streaming insurance analytics platform.

---

# 🏗 Architecture Overview

- **Policy Generator (Lambda - One Time)**
    - S3 → `raw/policy_master/`
- **FNOL Generator (Lambda - Scheduled)**
    - S3 → `raw/fnol_events/`
- **Claims Generator (Lambda - Triggered by FNOL)**
    - S3 → `raw/claims_history/`
- **Databricks Layers**
    - Bronze (Streaming Ingestion)
    - Silver (Cleaning + Data Quality + Dedup)
    - Gold (Risk Engine + Leakage Engine)
- **FastAPI Layer**
- **Dashboard UI**

---

# 🔄 Data Flow Explained

## 1️⃣ Data Generation (AWS Lambda)
- **Policy Generator (Static Master Data)**
    - Generates 5,000 policies
    - Messy data (L, K amounts, inconsistent dates, random strings, etc)
    - Base policies for claims and FNOL
- **FNOL Generator (Incremental)**
    - Generates First Notice of Loss records
    - Only for existing policies
    - Runs on schedule
- **Claims Generator (Event Driven)**
    - Triggered when FNOL file lands
    - Creates claim settlements linked to FNOL

**Stored As:**
- `s3://bucket name/`
    - `raw/fnol_events/`
    - `raw/claims_history/`
    - `raw/policy_master/`

---

# 🥉 Bronze Layer (Streaming Ingestion)

**Technology:**
- Databricks Auto Loader
- Structured Streaming
- Delta Lake

**Responsibilities:**
- Ingest raw JSON from S3
- Add metadata:
    - `source_file`
    - `ingest_date`
    - `start_ts`
    - `end_ts`
    - `is_current`
- Generate file-level ingestion metrics
- Handle schema evolution

**Config-Driven:** All paths, checkpoints, and tables are defined in CONFIG.

**Logging + Timing:**
- Each stream logs:
    - Start time
    - End time
    - Duration
    - Errors

---

# 🥈 Silver Layer (Cleaning + Data Quality)

**Responsibilities:**
- Parse inconsistent date formats
- Convert messy financial values (e.g., 3L → 300000, 1000K → 1000000)
- Normalize categorical values
- Deduplicate records

**Split:**
- Clean table
- Quarantine table

**Data Quality Checks:**
- **FNOL:**
    - `fnol_id` cannot be null
    - `policy_id` must exist
    - `claim_amount` must parse
- **Policy:**
    - `start_date` < `end_date`
    - `coverage_limit` must parse
- **Claims:**
    - `claim_id` cannot be null
    - `approved_amount` must parse

---

# 🥇 Gold Layer (Risk & Leakage Engine)

**Business Value Creation**

## 🎯 Risk Engine (FNOL Stage)
- **Risk features:**
    - Late reporting
    - High FNOL amount vs coverage
    - Risky loss type
    - Risky geography
    - Digital reporting channel
- **Risk Score Calculation:**
    
    Risk Score =
        (late_reporting × 30)
      + (high_amount × 25)
      + (risky_loss_type × 20)
      + (risky_geo × 15)
      + (paid_gt_approved × 40)
      + (paid_gt_coverage × 40)
    
- **Risk levels:**
    - HIGH ≥ 70
    - MEDIUM ≥ 40
    - LOW < 40

## 💸 Leakage Engine
- **Leakage flags:**
    - Paid amount > Approved amount
    - Paid amount > Coverage limit
    - Claim outside policy period
- **Leakage amount:**
    - `max(paid_amount - approved_amount, 0)`

**Final Output Table:**  
`gold.gold_claim_risk_summary`
- `claim_id`
- `fnol_id`
- `policy_id`
- `risk_score`
- `risk_level`
- `leakage_flag`
- `leakage_amount`
- `risk_reasons`

---

# 🌐 FastAPI Layer

- **Gold layer exposed via REST API**
- **Why FastAPI?**
    - High performance
    - Auto Swagger documentation
    - Production-ready
    - Lightweight

**API Endpoints:**
- Get High Risk Claims: `GET /claims/high-risk`
- Get Leakage Claims: `GET /claims/leakage`
- Get Summary Statistics: `GET /claims/summary`

**Returns:**
- Total claims
- High risk count
- Total leakage amount

**Running FastAPI:**
- Install Dependencies:  
    `pip install fastapi uvicorn pyspark`
- Start Server:  
    `uvicorn main:app --reload`
- Swagger UI:  
    `http://localhost:8000/docs`

---

# 📊 Dashboard

- Lightweight frontend dashboard consumes FastAPI endpoints

**Features:**
- Risk distribution visualization
- Leakage statistics
- High-risk claims table
- Claim investigation view
- Custom favicon
- Live API integration

**Flow:**  
Dashboard → FastAPI → Gold Tables

---

# ⚙️ Configuration System

- All hardcoding removed
- Centralized configuration:
    
    CONFIG = {
      streaming,
      tables,
      risk rules,
      weights,
      logging
    }
    
- **Benefits:**
    - Portable
    - Maintainable
    - Production-ready

---

# 🧾 Logging & Observability

- Python Logger
- Stream start/end tracking
- Execution duration tracking
- Structured error logging
- Try-catch blocks in critical steps

**Example log:**

[START] Stream write | entity=fnol
[END] Stream write | duration=2.53 sec


---

# 🧠 Business Value

This POC demonstrates:
- Real-time risk detection
- Automated financial leakage detection
- Event-driven architecture
- Streaming ingestion
- Data quality enforcement
- API-first design
- End-to-end automation
- Production-ready code structure

---

# 🛠 Requirements to Run This Project

1. **AWS Setup**
    - S3 bucket
    - 3 Lambda functions
    - S3 event trigger for claims Lambda
2. **Databricks Setup**
    - Unity Catalog enabled
    - Delta Lake
    - Auto Loader
    - Notebook jobs configured
3. **Python Environment**
    - fastapi
    - uvicorn
    - boto3
    - pyspark

---

# ▶️ How To Run End-to-End

1. Run Policy Lambda (one-time)
2. Start FNOL Lambda schedule
3. Claims Lambda triggers automatically
4. Run Bronze notebook
5. Run Silver notebooks
6. Run Gold notebook
7. Start FastAPI server
8. Open Dashboard

---

# 📂 Project Structure

- `/config`
- `/notebooks`
    - `/bronze`
    - `/silver`
    - `/gold`
- `/lambda`
    - `policy_generator.py`
    - `fnol_generator.py`
    - `claims_generator.py`
- `/api`
    - `main.py`
- `/dashboard`
    - `index.html`
- `README.md`

---

# 🔮 Future Enhancements

- Replace rule-based scoring with ML model
- Add CI/CD via Databricks Asset Bundles
- Containerize FastAPI
- Add MLflow model registry
- Add alerting system
- Add real-time streaming (no availableNow)

---

# 👨‍💻 Author

Built as a production-style POC to demonstrate:
- Data Engineering
- Streaming
- Insurance Domain Knowledge
- Risk Modeling
- API Development
- System Design

---

# 🏁 Final Note

This project is a complete insurance analytics platform prototype, not just a data pipeline.

It simulates:
- Real-world messy data
- Streaming ingestion
- Risk analytics
- Financial leakage detection
- API exposure
- Dashboard visualization