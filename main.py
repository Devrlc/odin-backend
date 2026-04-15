from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://app.odinflow.co.uk"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NOMIS_API_KEY = os.getenv("NOMIS_API_KEY")

@app.get("/")
def root():
    return {"status": "ODIN backend running"}

@app.get("/api/od-flows")
def get_od_flows(oa: str):
    url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_1228_1.data.json"

    params = {
        "date": "latest",
        "currently_residing_in": oa,
        "place_of_work": "TYPE297",
        "measures": "20100",
        "uid": NOMIS_API_KEY,
        "select": "currently_residing_in_code,place_of_work_code,obs_value",
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        print("Nomis status:", response.status_code)
        print("Nomis URL:", response.url)
        data = response.json()
        print("Nomis response keys:", list(data.keys()) if isinstance(data, dict) else "not a dict")

        flows = []
        if "obs" in data:
            for item in data["obs"]:
                origin = item.get("currently_residing_in", {}).get("geogcode", "")
                dest = item.get("place_of_work", {}).get("geogcode", "")
                count = item.get("obs_value", {}).get("value", 0)
                if count and count > 0:
                    flows.append({
                        "origin": origin,
                        "destination": dest,
                        "count": count
                    })

        total = sum(f["count"] for f in flows)
        for f in flows:
            f["percentage"] = round((f["count"] / total * 100), 2) if total > 0 else 0

        flows.sort(key=lambda x: x["count"], reverse=True)

        return {
            "origin_oa": oa,
            "total_trips": total,
            "flows": flows
        }

    except Exception as e:
        return {"error": str(e)}