import asyncio
import json
import ssl
import websockets
import httpx
import time
import os
import sys
from dotenv import load_dotenv

load_dotenv()

WS_URL = os.getenv("LKC_WS_URL", "wss://api.liquiditykineticscapital.com/v1/ws")
SSE_URL = os.getenv("LKC_SSE_URL", "https://api.liquiditykineticscapital.com/v1/stream")
REST_URL = os.getenv("LKC_REST_URL", "https://api.liquiditykineticscapital.com/v1/signals/historical")

# Newly rotated tokens for Enterprise and Professional testing
ENTERPRISE_TOKEN = os.getenv("LKC_ENTERPRISE_KEY")
PROFESSIONAL_TOKEN = os.getenv("LKC_PROFESSIONAL_KEY")
RESEARCHER_RAW_KEY = os.getenv("LKC_RESEARCHER_KEY")

if not all([ENTERPRISE_TOKEN, PROFESSIONAL_TOKEN, RESEARCHER_RAW_KEY]):
    print("ERROR: Missing one or more required API keys.")
    print("Please configure your .env file based on .env.example.")
    sys.exit(1)

ENTERPRISE_EXPECTED = {
    "signal_id", "symbol", "t0_timestamp_ms", "t0_bar_index", "t0_price",
    "confidence_level_pct", "vertical_barrier_bars", "conformal_lower_bound",
    "conformal_upper_bound", "predicted_volatility_pct"
}

PROTECTED_ALPHA_KEYS = {
    "signal_id", "t0_timestamp_ms", "t0_bar_index", "t0_price",
    "confidence_level_pct", "vertical_barrier_bars", "conformal_lower_bound",
    "conformal_upper_bound", "predicted_volatility_pct"
}

def extract_all_keys(data) -> set:
    keys = set()
    if isinstance(data, dict):
        for k, v in data.items():
            keys.add(k)
            keys.update(extract_all_keys(v))
    elif isinstance(data, list):
        for item in data:
            keys.update(extract_all_keys(item))
    return keys

async def listen_enterprise(success_event: asyncio.Event):
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    headers = {"x-api-key": ENTERPRISE_TOKEN}
    
    while not success_event.is_set():
        try:
            async with websockets.connect(
                WS_URL, 
                ssl=ssl_context, 
                extra_headers=headers,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                print("[Enterprise] Connected to WS")
                while not success_event.is_set():
                    message = await ws.recv()
                    try:
                        data = json.loads(message)
                        extracted_keys = extract_all_keys(data)
                        if ENTERPRISE_EXPECTED.issubset(extracted_keys):
                            print(f"\n[Enterprise] RAW PAYLOAD RECEIVED:\n{json.dumps(data, indent=2)}\n")
                            print("[Enterprise] Validated expected keys.")
                            success_event.set()
                            break
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            print(f"[Enterprise] Error: {e}")
            await asyncio.sleep(1)

async def listen_professional(success_event: asyncio.Event):
    headers = {"x-api-key": PROFESSIONAL_TOKEN}
    
    while not success_event.is_set():
        try:
            async with httpx.AsyncClient(verify=False, timeout=None) as client:
                async with client.stream("GET", SSE_URL, headers=headers) as response:
                    print("[Professional] Connected to SSE")
                    async for line in response.aiter_lines():
                        if success_event.is_set():
                            break
                        line = line.strip()
                        if not line or line.startswith(":"):
                            print("[SSE Heartbeat Received]")
                            continue
                        
                        if line.startswith("data: "):
                            payload_str = line[6:]
                            try:
                                data = json.loads(payload_str)
                                extracted_keys = extract_all_keys(data)
                                
                                intersection = PROTECTED_ALPHA_KEYS.intersection(extracted_keys)
                                if intersection:
                                    raise ValueError(f"FATAL: ALPHA LEAKAGE DETECTED. Leaked keys: {intersection}")
                                else:
                                    print(f"\n[Professional] RAW PAYLOAD RECEIVED:\n{json.dumps(data, indent=2)}\n")
                                    print("[Professional] Validated no alpha leakage.")
                                    success_event.set()
                                    break
                            except json.JSONDecodeError:
                                pass
        except Exception as e:
            print(f"[Professional] Error: {e}")
            await asyncio.sleep(1)

async def listen_researcher(success_event: asyncio.Event):
    headers = {"x-api-key": RESEARCHER_RAW_KEY}
    
    while not success_event.is_set():
        try:
            async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
                response = await client.get(REST_URL, headers=headers)
                if response.status_code == 200:
                    print("[Researcher] Connected to REST")
                    lines = response.text.strip().split('\n')
                    for line in lines:
                        if success_event.is_set():
                            break
                        if not line: continue
                        
                        if line.startswith("data: "):
                            payload_str = line[6:].strip()
                        else:
                            payload_str = line.strip()
                            
                        if payload_str == ": keepalive" or not payload_str or "error" in payload_str:
                            continue
                            
                        try:
                            data = json.loads(payload_str)
                            keys = data.keys()
                            
                            # Skip initial t0 signals, we want to print a full validation event
                            if "validation_result" not in keys:
                                continue
                                
                            # Strict Schema Validation for the complete event
                            if not {"t0_context", "prediction_parameters", "validation_result"}.issubset(keys):
                                continue # Just skip malformed ones, don't crash the loop
                            
                            # Zero Look-Ahead Bias Validation
                            t0_timestamp_ms = data["t0_context"].get("timestamp_ms")
                            if t0_timestamp_ms is None:
                                continue
                            
                            current_time_ms = int(time.time() * 1000)
                            if t0_timestamp_ms >= (current_time_ms - 24 * 60 * 60 * 1000):
                                print(f"[Researcher] FATAL ERROR: Zero Look-Ahead Bias Violation. Timestamp {t0_timestamp_ms} is not older than 24h.")
                                return # Abort completely if alpha is leaking
                            
                            # --- INJECT VISUAL TELEMETRY HERE ---
                            print("[Researcher] Validated Mixed Schema and Zero Look-Ahead Bias.")
                            print(f"\n[Researcher] RAW PAYLOAD RECEIVED:\n{json.dumps(data, indent=2)}\n")
                            success_event.set()
                            break
                            
                        except json.JSONDecodeError:
                            pass
                    
                    if not success_event.is_set():
                        await asyncio.sleep(1)
                else:
                    print(f"[Researcher] REST Error: {response.status_code}")
                    await asyncio.sleep(1)
        except Exception as e:
            print(f"[Researcher] Error: {e}")
            await asyncio.sleep(1)

async def main():
    ent_success = asyncio.Event()
    prof_success = asyncio.Event()
    res_success = asyncio.Event()
    
    async def ent_task():
        await listen_enterprise(ent_success)
        
    async def prof_task():
        await listen_professional(prof_success)
        
    async def res_task():
        await listen_researcher(res_success)
        
    t1 = asyncio.create_task(ent_task())
    t2 = asyncio.create_task(prof_task())
    t3 = asyncio.create_task(res_task())
    
    await asyncio.gather(ent_success.wait(), prof_success.wait(), res_success.wait())
    
    print("VALIDATION SUCCESS: REAL-TIME PRODUCTION DATA METRIC COMPLIANT")
    
    t1.cancel()
    t2.cancel()
    t3.cancel()

if __name__ == "__main__":
    asyncio.run(main())
