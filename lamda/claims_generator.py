import random
import string
from datetime import datetime, timedelta
import json
import boto3

s3 = boto3.client("s3")
BUCKET = "claims-risk-leakage"

# -------------------------
# Helpers
# -------------------------

def random_date(start_year=2018):
    start = datetime(start_year, 1, 1)
    end = datetime.utcnow()
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def messy_date(date_obj):
    formats = [
        "%Y-%m-%d",
        "%d/%m/%y",
        "%m-%d-%Y",
        "%b %d %Y",
        "%Y/%m/%d"
    ]
    return date_obj.strftime(random.choice(formats))


def messy_amount(value):
    formats = [
        str(value),
        f"{value:,}",
        f"{value/1000:.1f}k",
        "",
        None
    ]
    return random.choice(formats)


def random_string(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))





def fetch_existing_fnols():
    fnols = []

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="raw/fnol_events/"
    )

    if "Contents" not in response:
        return fnols

    for obj in response["Contents"]:
        key = obj["Key"]
        file_obj = s3.get_object(Bucket=BUCKET, Key=key)
        lines = file_obj["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            record = json.loads(line)

            if record.get("policy_id"):
                fnols.append({
                    "fnol_id": record["claim_id"],
                    "policy_id": record["policy_id"]
                })

    return fnols


def generate_claim_from_fnol(fnol):
    paid = random.randint(2000, 80000)

    return {
        "claim_id": f"CLM{random.randint(1000000,9999999)}",
        "fnol_id": fnol["fnol_id"],
        "policy_id": fnol["policy_id"],
        "loss_type": random.choice(["Collision", "Theft", "Fire", "Flood"]),
        "claim_status": random.choice(["Closed", "CLOSED", "Clsd"]),
        "paid_amount": messy_amount(paid),
        "approved_amount": messy_amount(paid + random.randint(-2000, 5000)),
        "settlement_date": messy_date(random_date()),
        "days_to_settle": str(random.randint(3,120)),
        "adjuster_id": f"ADJ{random.randint(100,999)}",
        "reopen_count": random.choice(["0", "1", None]),
        "litigation_flag": random.choice(["Y", "N"]),
        "source_system": random.choice(["LEGACY_SYS", "CORE_SYS"])
    }



# -------------------------
# S3 Upload Helper
# -------------------------

def upload_json_lines(key, records):
    body = "\n".join(json.dumps(r) for r in records)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)



def lambda_handler(event, context):

    # Get bucket and key of NEW file
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # Read only that file
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    lines = file_obj["Body"].read().decode("utf-8").splitlines()

    fnols = []

    for line in lines:
        record = json.loads(line)
        if record.get("policy_id"):
            fnols.append({
                "fnol_id": record["claim_id"],
                "policy_id": record["policy_id"]
            })

    claims = []

    for fnol in fnols:
        if random.random() < 0.7:
            claims.append(generate_claim_from_fnol(fnol))

    # Write claims file
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    claims_key = f"raw/claims_history/claims_{ts}.json"

    upload_json_lines(claims_key, claims)

    return {
        "statusCode": 200,
        "body": json.dumps({"claim_count": len(claims)})
    }
