import os
import time
import uuid
import random
import json
from datetime import datetime


SERVICES = ["frontend-service", "auth-service", "payment-service", "inventory-service"]
LEVELS = ["INFO", "INFO", "INFO", "WARNING", "INFO", "INFO", "ERROR"]

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

def generate_transaction():
  
    request_id = str(uuid.uuid4())
    trace_id = request_id
    user_id = random.randint(1000, 9999)
    amount = round(random.uniform(5.00, 500.00), 2)
    timestamp = datetime.utcnow().isoformat() + "Z"

    flow = [
        {
            "timestamp": timestamp,
            "service_name": "frontend-service",
            "level": "INFO",
            "message": f"Received checkout request from user_id={user_id} for amount=${amount}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"user_id": user_id, "amount": amount}
        },
        {
            "timestamp": timestamp,
            "service_name": "auth-service",
            "level": "INFO",
            "message": f"Successfully authenticated token for user_id={user_id}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"auth_status": "success", "user_id": user_id}
        }
    ]

    # Add random chance of payment failure
    payment_failed = random.choice([False, False, False, False, True]) # 20% chance of failure
    if payment_failed:
        flow.append({
            "timestamp": timestamp,
            "service_name": "payment-service",
            "level": "ERROR",
            "message": f"Payment processing failed for user_id={user_id} on transaction amount=${amount}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"error": "INSUFFICIENT_FUNDS", "user_id": user_id, "amount": amount}
        })
        flow.append({
            "timestamp": timestamp,
            "service_name": "frontend-service",
            "level": "WARNING",
            "message": f"Checkout transaction aborted due to payment failure for user_id={user_id}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"user_id": user_id, "status": "aborted"}
        })
    else:
        flow.append({
            "timestamp": timestamp,
            "service_name": "payment-service",
            "level": "INFO",
            "message": f"Payment of ${amount} captured successfully for user_id={user_id}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"payment_status": "captured", "user_id": user_id, "amount": amount}
        })
        flow.append({
            "timestamp": timestamp,
            "service_name": "inventory-service",
            "level": "INFO",
            "message": f"Successfully reserved stock for order user_id={user_id}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"stock_status": "reserved", "quantity": 1}
        })
        flow.append({
            "timestamp": timestamp,
            "service_name": "frontend-service",
            "level": "INFO",
            "message": f"Checkout completed successfully for user_id={user_id}",
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": {"user_id": user_id, "status": "completed"}
        })

   
    if random.random() < 0.15:
        flow.append({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service_name": random.choice(SERVICES),
            "level": random.choice(["WARNING", "ERROR", "CRITICAL"]),
            "message": "High CPU utilization detected - thread pool saturation warning.",
            "request_id": str(uuid.uuid4()),
            "trace_id": str(uuid.uuid4()),
            "payload": {"cpu_percent": random.randint(85, 99)}
        })

    return flow

def main():
    print(f"Starting Log Generator. Appending to {LOG_FILE}...")
    try:
        while True:
  
            transaction_logs = generate_transaction()
            with open(LOG_FILE, "a") as f:
                for log in transaction_logs:
                    f.write(json.dumps(log) + "\n")
            

            time.sleep(2)
    except KeyboardInterrupt:
        print("Log Generator stopped.")

if __name__ == "__main__":
    main()
