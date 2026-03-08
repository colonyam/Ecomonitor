import asyncio
import sqlite3
import random
import json
from datetime import datetime
from contextlib import asynccontextmanager
from threading import Thread
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

# --- Database Setup ---
DATABASE = "waste_management.db"

def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            fill_level INTEGER DEFAULT 0,
            status TEXT DEFAULT 'normal', -- normal, warning, critical
            last_updated TEXT
        )
    """)
    
    # Seed data if empty
    cursor.execute("SELECT COUNT(*) FROM bins")
    if cursor.fetchone()[0] == 0:
        bins = [
            ("Central Park Bin A", 40.785091, -73.968285),
            ("Broadway & 5th", 40.712776, -74.005974),
            ("Shopping Mall East", 40.758896, -73.985130),
            ("Station Terminus", 40.750504, -73.993439),
            ("City Hospital Rear", 40.741895, -73.989308),
            ("Tech District Hub", 40.730610, -73.935242),
        ]
        for name, lat, lng in bins:
            cursor.execute(
                "INSERT INTO bins (name, lat, lng, fill_level, last_updated) VALUES (?, ?, ?, ?, ?)",
                (name, lat, lng, random.randint(20, 90), datetime.now().isoformat())
            )
    conn.commit()
    conn.close()

# --- Pydantic Models ---
class Bin(BaseModel):
    id: int
    name: str
    lat: float
    lng: float
    fill_level: int
    status: str
    last_updated: str

class FillUpdate(BaseModel):
    bin_id: int
    fill_level: int

# --- IoT Simulator ---
def iot_simulator():
    """Background thread simulating sensors sending data"""
    while True:
        try:
            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()
            cursor.execute("SELECT id, fill_level FROM bins")
            bins = cursor.fetchall()
            
            # Randomly update some bins
            for bin_id, current_level in bins:
                if random.random() > 0.7: # 30% chance to update
                    change = random.randint(-5, 15) # Can be negative (simulating emptying) or positive
                    new_level = max(0, min(100, current_level + change))
                    
                    status = "normal"
                    if new_level > 80: status = "critical"
                    elif new_level > 50: status = "warning"
                    
                    cursor.execute(
                        "UPDATE bins SET fill_level = ?, status = ?, last_updated = ? WHERE id = ?",
                        (new_level, status, datetime.now().isoformat(), bin_id)
                    )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Simulator error: {e}")
        time.sleep(5) # Update every 5 seconds

# --- API Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    thread = Thread(target=iot_simulator, daemon=True)
    thread.start()
    yield

app = FastAPI(title="Smart Waste Management API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---
@app.get("/")
def read_root():
    return {"message": "Smart Waste API Running", "status": "operational"}

@app.get("/api/bins", response_model=List[Bin])
def get_bins():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM bins")
    bins = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bins

@app.get("/api/stats")
def get_stats():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT AVG(fill_level) FROM bins")
    avg_fill = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM bins WHERE status = 'critical'")
    critical_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM bins WHERE status = 'warning'")
    warning_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(fill_level) FROM bins")
    total_waste = cursor.fetchone()[0] or 0
    
    conn.close()
    
    return {
        "average_fill_level": round(avg_fill, 2),
        "critical_bins": critical_count,
        "warning_bins": warning_count,
        "total_waste_units": total_waste,
        "efficiency_score": round(100 - avg_fill, 2) # Mock efficiency metric
    }

@app.post("/api/collect/{bin_id}")
def collect_bin(bin_id: int):
    """Simulates a truck collecting/emptying a specific bin"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE bins SET fill_level = 0, status = 'normal', last_updated = ? WHERE id = ?",
        (datetime.now().isoformat(), bin_id)
    )
    conn.commit()
    conn.close()
    return {"message": f"Bin {bin_id} collected successfully", "status": "success"}

@app.get("/api/route")
def get_optimized_route():
    """
    Simple optimization: Returns bin IDs sorted by fill_level DESC 
    (Nearest neighbor algo would go here in a real GPS app)
    """
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, lat, lng, fill_level FROM bins WHERE fill_level > 50 ORDER BY fill_level DESC")
    route = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"optimized_route": route, "stops": len(route)}

if __name__ == "__main__":
    import uvicorn
    print("Starting Smart Waste Management Server...")
    print("Dashboard URL: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
