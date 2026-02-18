import random
import string
import json
import uuid
from datetime import datetime, timedelta
import boto3

s3 = boto3.client("s3")
BUCKET = "claims-risk-leakage"


# ---------------- helpers ----------------

def random_date(start_year=2023):
    start = datetime(start_year, 1, 1)
    end = datetime.utcnow()
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def messy_date(date_obj):
    formats = ["%Y-%m-%d", "%d/%m/%y", "%b %d %Y"]
    return date_obj.strftime(random.choice(formats))


def messy_amount(value):
    return random.choice([
        str(value),
        f"{value:,}",
        f"{value/1000:.1f}k",
        "",
        None
    ])


def random_string(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


# ---------------- Fetch Policies ----------------

def fetch_existing_policies():
    policies = []

    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key="raw/policy_master/policy_master.json"
        )

        lines = obj["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            record = json.loads(line)
            if "policy_id" in record:
                policies.append(record["policy_id"])

    except Exception as e:
        print("Error fetching policies:", str(e))

    return policies


# ---------------- FNOL generator ----------------

def generate_fnol(policy_ids):
    loss_date = random_date()
    claim_amt = random.randint(3000, 100000)

    return {
        "claim_id": f"FNOL{uuid.uuid4().hex[:10]}",
        "policy_id": random.choice(policy_ids) if policy_ids else None,
        "policy_number_legacy": random_string(),
        "loss_type": random.choice(["Collision", "Fire", "Theft", "Flood"]),
        "loss_description": random.choice([
            "Rear-end accident",
            "House fire",
            "Water damage"
        ]),
        "loss_date": messy_date(loss_date),
        "reported_date": messy_date(datetime.utcnow()),
        "claim_amount": messy_amount(claim_amt),
        "incident_state": random.choice(["KA","MH","TN","AP"]),
        "incident_city": random.choice([
            "Bangalore","Mumbai","Chennai","Hyderabad"
        ]),
        "incident_zip": random.choice(["560001",560001,"0560001"]),
        "reporting_channel": random.choice(["App","Call","Agent"]),
        "agent_id": random.choice([f"AG{random.randint(10,99)}", None]),
        "device_type": random.choice(["Android","iOS","Web"])
    }


# ---------------- Lambda handler ----------------

def lambda_handler(event, context):

    print("Starting FNOL generation")

    policy_ids = fetch_existing_policies()
    print("Policies fetched:", len(policy_ids))

    if not policy_ids:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "No policies found"})
        }

    batch_size = random.randint(200, 500)

    records = [generate_fnol(policy_ids) for _ in range(batch_size)]

    body = "\n".join(json.dumps(r) for r in records)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"raw/fnol_events/fnol_{ts}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body
    )

    print(f"Generated {batch_size} FNOL events → {key}")

    return {
        "statusCode": 200,
        "body": json.dumps({"records": batch_size})
    }
