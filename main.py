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
        data = response.json()
        flows = []
        if "obs" in data:
            for item in data["obs"]:
                origin = item.get("currently_residing_in", {}).get("geogcode", "")
                dest = item.get("place_of_work", {}).get("geogcode", "")
                count = item.get("obs_value", {}).get("value", 0)
                if count and count > 0:
                    flows.append({"origin": origin, "destination": dest, "count": count})
        total = sum(f["count"] for f in flows)
        for f in flows:
            f["percentage"] = round((f["count"] / total * 100), 2) if total > 0 else 0
        flows.sort(key=lambda x: x["count"], reverse=True)
        return {"origin_oa": oa, "total_trips": total, "flows": flows}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/mode-share")
def get_mode_share(msoa: str):
    url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_1208_1.data.json"
    params = {
        "date": "latest",
        "usual_residence": msoa,
        "place_of_work": "TYPE297",
        "transport_powpew11": "0,1,2,3,4,5,6,7,8,9,10,11",
        "measures": "20100",
        "uid": NOMIS_API_KEY,
        "select": "transport_powpew11_code,transport_powpew11_name,obs_value",
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        mode_names = {
            0: "All", 1: "Work from home", 2: "Metro/tram",
            3: "Train", 4: "Bus", 5: "Taxi", 6: "Motorcycle",
            7: "Car driver", 8: "Car passenger", 9: "Bicycle",
            10: "On foot", 11: "Other"
        }
        mode_totals = {}
        total = 0
        if "obs" in data:
            for item in data["obs"]:
                code = item.get("transport_powpew11", {}).get("value", -1)
                count = item.get("obs_value", {}).get("value", 0) or 0
                if code == 0:
                    total += count
                elif code in range(1, 12):
                    mode_totals[code] = mode_totals.get(code, 0) + count
        if total == 0:
            total = sum(mode_totals.values())
        modes = []
        for code in sorted(mode_totals.keys()):
            count = mode_totals[code]
            modes.append({
                "code": code,
                "name": mode_names.get(code, "Unknown"),
                "count": count,
                "percentage": round(count / total * 100, 1) if total > 0 else 0
            })
        return {"msoa": msoa, "total": total, "modes": modes}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/oa-to-msoa")
def get_oa_to_msoa(oa: str):
    url = (
        "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
        "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
        "FeatureServer/0/query"
        "?where=" + f"OA11CD='{oa}'" +
        "&outFields=OA11CD,MSOA11CD,MSOA11NM"
        "&f=json"
        "&resultRecordCount=1"
    )
    try:
        resp = requests.get(url, timeout=15)
        data = resp.json()
        if "features" in data and len(data["features"]) > 0:
            attrs = data["features"][0]["attributes"]
            return {
                "oa": oa,
                "msoa": attrs.get("MSOA11CD"),
                "msoa_name": attrs.get("MSOA11NM")
            }
        return {"oa": oa, "msoa": None}
    except Exception as e:
        return {"error": str(e)}