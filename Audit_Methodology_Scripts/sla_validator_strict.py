import asyncio
import websockets
import ssl
import time
import aiohttp
import os
import sys
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("LKC_WS_URL", "wss://api.liquiditykineticscapital.com/v1/ws")
REST_URL = os.getenv("LKC_REST_URL", "https://api.liquiditykineticscapital.com/v1/signals/historical")

ENTERPRISE_RAW_KEY = os.getenv("LKC_ENTERPRISE_KEY")
PROFESSIONAL_RAW_KEY = os.getenv("LKC_PROFESSIONAL_KEY")
RESEARCHER_RAW_KEY = os.getenv("LKC_RESEARCHER_KEY")

if not all([ENTERPRISE_RAW_KEY, PROFESSIONAL_RAW_KEY, RESEARCHER_RAW_KEY]):
    print("ERROR: Missing one or more required API keys.")
    print("Please configure your .env file based on .env.example.")
    sys.exit(1)

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async def test_professional_tier():
    print("--- Test 1: Professional Tier Enforcement ---")
    try:
        ws = await asyncio.wait_for(
            websockets.connect(URI, extra_headers={"X-API-Key": PROFESSIONAL_RAW_KEY}, ssl=ssl_context),
            timeout=5.0
        )
        await ws.close()
        raise SystemError("CRITICAL FAIL: Connected successfully with Professional key. Expected 403.")
    except websockets.exceptions.InvalidStatusCode as e:
        if e.status_code == 403:
            print(f"PASS: Connection rejected with {e.status_code} as expected.")
            return True
        elif e.status_code == 500:
            raise SystemError("CRITICAL FAIL: Gateway returned unexpected status code.")
        else:
            raise SystemError(f"CRITICAL FAIL: Gateway returned unexpected status code {e.status_code}.")
    except Exception as e:
        if isinstance(e, SystemError):
            raise
        raise SystemError(f"CRITICAL FAIL: Unexpected error: {e}")

async def test_enterprise_concurrency():
    print("\n--- Test 2: Enterprise Concurrency Limit (Max 3) ---")
    active_connections = []
    
    try:
        for i in range(1, 5):
            print(f"Attempting connection {i}...")
            try:
                ws = await asyncio.wait_for(
                    websockets.connect(URI, extra_headers={"X-API-Key": ENTERPRISE_RAW_KEY}, ssl=ssl_context),
                    timeout=5.0
                )
                
                # Wait a moment to see if it gets closed immediately after opening
                try:
                    await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass # No message received, but connection is still open
                except websockets.exceptions.ConnectionClosed as e:
                    if i == 4:
                        if e.code in [1008, 403, 429]:
                            print(f"PASS: Connection 4 closed immediately. Code: {e.code}, Reason: {e.reason}")
                            break
                        else:
                            raise SystemError(f"CRITICAL FAIL: Connection 4 closed with unexpected code {e.code}.")
                    else:
                        raise SystemError(f"CRITICAL FAIL: Connection {i} closed unexpectedly. Code: {e.code}.")
                
                if i == 4:
                    raise SystemError("CRITICAL FAIL: Connection 4 succeeded. Expected rejection.")
                else:
                    print(f"PASS: Connection {i} succeeded.")
                    active_connections.append(ws)
                    
            except websockets.exceptions.InvalidStatusCode as e:
                if e.status_code == 500:
                    raise SystemError("CRITICAL FAIL: Gateway crashed under concurrency limit.")
                
                if i == 4:
                    if e.status_code in [403, 429, 1008]:
                        print(f"PASS: Connection 4 rejected with status {e.status_code}.")
                        break
                    else:
                        raise SystemError(f"CRITICAL FAIL: Connection 4 rejected with unexpected status {e.status_code}.")
                else:
                    raise SystemError(f"CRITICAL FAIL: Connection {i} rejected with status {e.status_code}.")
            except Exception as e:
                if isinstance(e, SystemError):
                    raise
                raise SystemError(f"CRITICAL FAIL: Connection {i} failed unexpectedly: {e}")
            
            if i < 4:
                await asyncio.sleep(1.0)
                
    finally:
        print("\nCleaning up active connections...")
        for i, ws in enumerate(active_connections):
            await ws.close()
            print(f"Closed connection {i+1}")
            
    return True

async def test_rest_rate_limit():
    print("\n--- Test 3: REST API Rate Limit (Max 10) ---")
    headers = {"X-API-Key": RESEARCHER_RAW_KEY}
    
    async def make_request(session, i):
        try:
            async with session.get(REST_URL, headers=headers) as response:
                return i, response.status
        except Exception as e:
            return i, str(e)

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [make_request(session, i) for i in range(1, 16)]
        results = await asyncio.gather(*tasks)
        
        success_count = 0
        rate_limited_count = 0
        
        for i, status in results:
            if status == 200:
                success_count += 1
            elif status == 429:
                rate_limited_count += 1
            else:
                raise SystemError(f"CRITICAL FAIL: Request {i} returned unexpected status {status}.")
                
        print(f"Successful requests: {success_count}")
        print(f"Rate limited requests (429): {rate_limited_count}")
        
        if success_count == 10 and rate_limited_count == 5:
            print("PASS: Gateway successfully handled first 10 requests and dropped the excess with 429.")
            return True
        else:
            raise SystemError(f"CRITICAL FAIL: Expected 10 successes and 5 rate limits, got {success_count} successes and {rate_limited_count} rate limits.")

async def main():
    print("Starting Deterministic SLA Validation...\n")
    
    results = {
        "Test 1 (Professional Tier Block)": {"status": "FAIL", "error": None},
        "Test 2 (Enterprise Concurrency)": {"status": "FAIL", "error": None},
        "Test 3 (REST API 429 Limit)": {"status": "FAIL", "error": None}
    }

    try:
        await test_professional_tier()
        results["Test 1 (Professional Tier Block)"]["status"] = "PASS"
    except Exception as e:
        results["Test 1 (Professional Tier Block)"]["error"] = str(e)

    try:
        await test_enterprise_concurrency()
        results["Test 2 (Enterprise Concurrency)"]["status"] = "PASS"
    except Exception as e:
        results["Test 2 (Enterprise Concurrency)"]["error"] = str(e)

    try:
        await test_rest_rate_limit()
        results["Test 3 (REST API 429 Limit)"]["status"] = "PASS"
    except Exception as e:
        results["Test 3 (REST API 429 Limit)"]["error"] = str(e)

    print("\n=========================================")
    print("         SLA ENFORCEMENT REPORT          ")
    print("=========================================")
    
    all_passed = True
    for test_name, result in results.items():
        status = result["status"]
        if status == "PASS":
            print(f"{test_name}: PASS")
        else:
            all_passed = False
            print(f"{test_name}: FAIL")
            print(f"  -> Error: {result['error']}")
            
    print("=========================================")
    if all_passed:
        print("CONCLUSION: Gateway successfully recognized tiers and enforced concurrency limits.")
    else:
        print("CONCLUSION: SLA Enforcement FAILED.")

if __name__ == "__main__":
    asyncio.run(main())