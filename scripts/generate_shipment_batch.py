"""
NaijaMove Logistics Pipeline - Transactional Data Generator
Author: Yusuf Muhammad Musa
Description: Simulates deterministic daily batches of logistics bookings with a controlled 
             corruption budget for pipeline robustness testing.
"""

import json
import random
import sys
from datetime import datetime, timedelta

# Static Reference pools for realistic data simulation
NIGERIAN_STATES = [
    "Lagos", "Abuja", "Kano", "Oyo", "Rivers", "Kaduna", "Anambra", 
    "Edo", "Delta", "Ogun", "Kwara", "Enugu", "Plateau", "Bauchi"
]

VEHICLE_TYPES = ["10-Ton Container", "5-Ton Box Truck", "Flatbed Trailer"]
STATUS_CODES = ["PENDING", "IN_TRANSIT", "DELAYED", "DELIVERED", "CANCELLED"]

def generate_deterministic_seed(date_str: str) -> int:
    """Converts an ISO date string (YYYY-MM-DD) into a unique integer seed."""
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        return int(parsed_date.strftime("%Y%m%d"))
    except ValueError:
        # Fallback if an unexpected string format passes through
        return 20260717 

def generate_shipment_batch(output_path: str, execution_date_str: str, record_count: int = 100):
    """
    Generates a NDJSON batch of shipment data.
    Anchors randomness to the execution date and injects a 5% corruption rate.
    """
    # Establish absolute determinism for this execution date run
    seed_value = generate_deterministic_seed(execution_date_str)
    random.seed(seed_value)
    
    base_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    records = []
    
    print(f"[GENERATOR] Generating {record_count} records with seed {seed_value} for date {execution_date_str}...")

    for i in range(1, record_count + 1):
        # Generate clean, standard baseline variables
        shipment_id = f"NM-{execution_date_str.replace('-', '')}-{i:04d}"
        origin = random.choice(NIGERIAN_STATES)
        destination = random.choice([s for s in NIGERIAN_STATES if s != origin])
        
        # Simulate business transaction timestamp within the execution day
        random_minutes = random.randint(0, 1439)
        booking_time = (base_date + timedelta(minutes=random_minutes)).isoformat()
        
        weight = round(random.uniform(50.0, 12000.0), 2)
        vehicle = random.choice(VEHICLE_TYPES)
        status = random.choice(STATUS_CODES)
        
        # Generate standard Nigerian commercial vehicle plate format (e.g., LSR-123XA)
        plate_number = f"{random.choice(['LSR', 'ABJ', 'KND', 'GGE'])}-{random.randint(100, 999)}{random.choice(['XA', 'KY', 'MZ'])}"
        
        # Base schema record payload
        record = {
            "shipment_id": shipment_id,
            "booking_timestamp": booking_time,
            "origin_state": origin,
            "destination_state": destination,
            "weight_kg": str(weight),
            "assigned_vehicle_type": vehicle,
            "truck_plate_number": plate_number,
            "delivery_status": status
        }
        
        # -------------------------------------------------------------
        # CORRUPTION ENGINE (5% Shared Budget across 6 Anomaly Types)
        # -------------------------------------------------------------
        if random.random() < 0.05:
            anomaly_type = random.randint(1, 6)
            
            if anomaly_type == 1:
                # 1. Structural Corruption: Inject unparseable alphanumeric garbage into weight
                record["weight_kg"] = "1250kg_UNKNOWN"
                
            elif anomaly_type == 2:
                # 2. Schema Deviation: Missing/Null critical operational field
                record["delivery_status"] = None
                
            elif anomaly_type == 3:
                # 3. Integrity Malformation: Malformed plate number format
                record["truck_plate_number"] = "INVALID_PLATE_12345"
                
            elif anomaly_type == 4:
                # 4. Out-of-Bounds Logic: Impossible destination context
                record["destination_state"] = "Texas_USA"
                
            elif anomaly_type == 5:
                # 5. Timestamp Breakage: Unparseable custom date string format
                record["booking_timestamp"] = "17-07-2026 14:23:11"
                
            elif anomaly_type == 6:
                # 6. Primary Key Duplication: Force an identical ID collision downstream
                record["shipment_id"] = f"NM-{execution_date_str.replace('-', '')}-0001"

        records.append(record)

    # Write file out safely as Line-Delimited JSON (NDJSON)
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
            
    print(f"[GENERATOR] Successfully exported batch to local storage destination: {output_path}")


# Executable logic anchor block if run outside Airflow structure directly
if __name__ == "__main__":
    import os
    
    # Defaults to current runtime date context if no terminal args passed
    default_date = "2026-07-17"
    default_filename = f"shipments_{default_date}.ndjson"
    
    # Establish dynamic target folder relative to script location
    target_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(target_dir, exist_ok=True)
    
    output_target = os.path.join(target_dir, default_filename)
    
    generate_shipment_batch(
        output_path=output_target,
        execution_date_str=default_date,
        record_count=100
    )
