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



# -------------------------
# Policy Generator
# -------------------------

def generate_policy():
    policy_id = f"POL{random.randint(100000,999999)}"
    start_date = random_date(2015)
    end_date = start_date + timedelta(days=365 * random.randint(1,5))
    coverage = random.choice([300000, 500000, 1000000])

    return {
        "policy_id": policy_id,
        "policy_type": random.choice(["Auto", "Home"]),
        "policy_status": random.choice(["ACTIVE", "Active", "LAPSED"]),
        "policy_start_date": messy_date(start_date),
        "policy_end_date": messy_date(end_date),
        "policy_tenure_years": str(random.randint(1,5)),
        "coverage_limit": random.choice([
            str(coverage),
            f"{coverage//1000}K",
            f"{coverage//100000}L"
        ]),
        "deductible": random.choice(["1000", "2000", None]),
        "premium_amount": messy_amount(random.randint(8000, 25000)),
        "risk_region": random.choice(["NORTH", "SOUTH", "EAST", "WEST"]),
        "underwriter_id": f"UW{random.randint(10,99)}",
        "product_code": random_string(),
        "distribution_channel": random.choice(["Agent", "Online", "Branch"]),
        "auto_renewal_flag": random.choice(["Y", "N", None])
    }




# -------------------------
# S3 Upload Helper
# -------------------------

def upload_json_lines(key, records):
    body = "\n".join(json.dumps(r) for r in records)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)


# -------------------------
# Lambda Handler
# -------------------------

def lambda_handler(event, context):

    policies = [generate_policy() for _ in range(10000)]

    upload_json_lines(
        "raw/policy_master/policy_master.json",
        policies
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"policy_count": len(policies)})
    }
