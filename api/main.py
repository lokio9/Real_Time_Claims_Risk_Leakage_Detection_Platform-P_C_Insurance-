from fastapi import FastAPI, HTTPException
from databricks import sql
import os
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI(
    title="Claims Risk & Leakage API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================
# Databricks Config
# =============================

DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

# =============================
# Endpoints
# =============================

@app.get("/")
def root():
    return {"message": "Claims Risk Intelligence API Running"}



@app.get("/dashboard/kpis")
def get_kpis():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_claims,
                        SUM(CASE WHEN s.risk_level='HIGH' THEN 1 ELSE 0 END) as high_risk_claims,
                        SUM(s.leakage_amount) as total_leakage_exposure,
                        AVG(f.days_to_report) as avg_days_to_report
                    FROM claims_leakage.gold.gold_claim_risk_summary s
                    JOIN claims_leakage.gold.gold_claim_risk_features f
                    ON s.fnol_id = f.fnol_id

                """)
                row = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/claims/high-risk")
def get_high_risk(limit: int = 20):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT *
                    FROM claims_leakage.gold.gold_claim_risk_summary
                    WHERE risk_level = 'HIGH'
                    ORDER BY risk_score DESC
                    LIMIT {limit}
                """)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/claims/{claim_id}")
def get_claim(claim_id: str):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT *
                    FROM claims_leakage.gold.gold_claim_risk_summary
                    WHERE claim_id = '{claim_id}'
                """)
                row = cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Claim not found")
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
