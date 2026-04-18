from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import os
import math
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


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, list):
        cleaned = [clean_value(i) for i in v]
        return cleaned[0] if len(cleaned) == 1 else cleaned
    return v


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
            return {"oa": oa, "msoa": attrs.get("MSOA11CD"), "msoa_name": attrs.get("MSOA11NM")}
        return {"oa": oa, "msoa": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/road-network")
def get_road_network(pin_lat: float, pin_lng: float, radius_m: int = 1000):
    try:
        import osmnx as ox

        G = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)
        nodes, edges = ox.graph_to_gdfs(G)
        edges_reset = edges.reset_index()

        features = []
        for _, row in edges_reset.iterrows():
            name = clean_value(row.get('name'))
            highway = clean_value(row.get('highway'))
            maxspeed = clean_value(row.get('maxspeed'))
            length = row.get('length', 0)
            try:
                length = float(length)
                if math.isnan(length):
                    length = 0
            except:
                length = 0

            features.append({
                "type": "Feature",
                "properties": {"name": name, "highway": highway, "length": length, "maxspeed": maxspeed, "oneway": bool(row.get('oneway', False))},
                "geometry": {"type": "LineString", "coordinates": list(row['geometry'].coords)}
            })

        node_features = []
        nodes_reset = nodes.reset_index()
        for _, row in nodes_reset.iterrows():
            node_features.append({
                "type": "Feature",
                "properties": {"osmid": int(row['osmid'])},
                "geometry": {"type": "Point", "coordinates": [row['geometry'].x, row['geometry'].y]}
            })

        return {
            "edges": {"type": "FeatureCollection", "features": features},
            "nodes": {"type": "FeatureCollection", "features": node_features},
            "edge_count": len(features),
            "node_count": len(node_features)
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/assign-trips")
def assign_trips(pin_lat: float, pin_lng: float, radius_m: int = 1000, vehicle_trips: int = 0, flows: str = "", access_lat: float = None, access_lng: float = None):
    try:
        import osmnx as ox
        import networkx as nx
        from shapely.geometry import Point, LineString

        G = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)
        G = ox.add_edge_speeds(G)
        G = ox.add_edge_travel_times(G)

        # Determine origin node
        if access_lat and access_lng:
            # Find nearest edge
            u, v, k = ox.nearest_edges(G, access_lng, access_lat)
            edge_data = G[u][v][k]

            # Snap to nearest point on edge
            if 'geometry' in edge_data:
                line = edge_data['geometry']
            else:
                line = LineString([(G.nodes[u]['x'], G.nodes[u]['y']),
                                   (G.nodes[v]['x'], G.nodes[v]['y'])])

            snapped = line.interpolate(line.project(Point(access_lng, access_lat)))
            new_lng, new_lat = snapped.x, snapped.y

            # Add new node
            new_node_id = 999999999
            G.add_node(new_node_id, x=new_lng, y=new_lat)

            # Get travel time from edge
            speed = edge_data.get('speed_kph', 50)
            if not speed or (isinstance(speed, float) and math.isnan(speed)):
                speed = 50

            # Distance from new node to u and v
            def dist_m(lng1, lat1, lng2, lat2):
                return ((lng2 - lng1) ** 2 + (lat2 - lat1) ** 2) ** 0.5 * 111320

            dist_u = dist_m(new_lng, new_lat, G.nodes[u]['x'], G.nodes[u]['y'])
            dist_v = dist_m(new_lng, new_lat, G.nodes[v]['x'], G.nodes[v]['y'])
            tt_u = dist_u / (speed * 1000 / 3600)
            tt_v = dist_v / (speed * 1000 / 3600)

            # Connect new node to both ends of the split edge
            G.add_edge(new_node_id, u, 0, travel_time=tt_u, length=dist_u,
                      geometry=LineString([(new_lng, new_lat), (G.nodes[u]['x'], G.nodes[u]['y'])]))
            G.add_edge(u, new_node_id, 0, travel_time=tt_u, length=dist_u,
                      geometry=LineString([(G.nodes[u]['x'], G.nodes[u]['y']), (new_lng, new_lat)]))
            G.add_edge(new_node_id, v, 0, travel_time=tt_v, length=dist_v,
                      geometry=LineString([(new_lng, new_lat), (G.nodes[v]['x'], G.nodes[v]['y'])]))
            G.add_edge(v, new_node_id, 0, travel_time=tt_v, length=dist_v,
                      geometry=LineString([(G.nodes[v]['x'], G.nodes[v]['y']), (new_lng, new_lat)]))

            origin_node = new_node_id
            print(f"Access node added at ({new_lat:.5f},{new_lng:.5f}), connected to nodes {u} and {v}")

        else:
            origin_node = ox.nearest_nodes(G, pin_lng, pin_lat)
            print(f"Using nearest node to pin: {origin_node}")

        # Parse flows
        flow_list = []
        if flows:
            for item in flows.split(','):
                parts = item.strip().split(':')
                if len(parts) == 2:
                    try:
                        flow_list.append({'msoa': parts[0].strip(), 'percentage': float(parts[1].strip())})
                    except:
                        pass

        # Initialise edge trip counts
        edge_trips = {}
        for uu, vv, kk in G.edges(keys=True):
            edge_trips[(uu, vv, kk)] = 0

        total_assigned = 0

        for flow in flow_list:
            msoa = flow['msoa']
            pct = flow['percentage']
            trips_to_dest = round(vehicle_trips * pct / 100)
            if trips_to_dest == 0:
                continue

            try:
                centroid_url = (
                    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
                    "MSOA_Dec_2011_PWC_in_England_and_Wales_2022/FeatureServer/0/query"
                    f"?where=msoa11cd='{msoa}'&outFields=msoa11cd&returnGeometry=true&outSR=4326&f=json&resultRecordCount=1"
                )
                centroid_resp = requests.get(centroid_url, timeout=10)
                centroid_data = centroid_resp.json()

                if not centroid_data.get('features'):
                    continue

                feature = centroid_data['features'][0]
                geom = feature.get('geometry', {})
                dest_lng = geom.get('x')
                dest_lat = geom.get('y')

                if not dest_lat or not dest_lng:
                    continue

                dest_node = ox.nearest_nodes(G, dest_lng, dest_lat)

                if dest_node == origin_node:
                    continue

                try:
                    path = nx.shortest_path(G, origin_node, dest_node, weight='travel_time')
                except nx.NetworkXNoPath:
                    try:
                        G_undirected = G.to_undirected()
                        path = nx.shortest_path(G_undirected, origin_node, dest_node, weight='travel_time')
                    except:
                        continue

                if len(path) < 2:
                    continue

                for i in range(len(path) - 1):
                    uu, vv = path[i], path[i + 1]
                    if G.has_edge(uu, vv):
                        kk = min(G[uu][vv].keys())
                        edge_trips[(uu, vv, kk)] = edge_trips.get((uu, vv, kk), 0) + trips_to_dest

                total_assigned += trips_to_dest

            except Exception as e:
                print(f"Error routing to {msoa}: {e}")
                continue

        # Build GeoJSON output
        features = []
        for uu, vv, kk, data in G.edges(keys=True, data=True):
            trips = edge_trips.get((uu, vv, kk), 0)

            if 'geometry' in data:
                coords = list(data['geometry'].coords)
            else:
                coords = [(G.nodes[uu]['x'], G.nodes[uu]['y']),
                          (G.nodes[vv]['x'], G.nodes[vv]['y'])]

            name = clean_value(data.get('name'))
            highway = clean_value(data.get('highway'))
            maxspeed = clean_value(data.get('maxspeed'))
            length = data.get('length', 0)
            try:
                length = float(length)
                if math.isnan(length):
                    length = 0
            except:
                length = 0

            features.append({
                "type": "Feature",
                "properties": {
                    "name": name, "highway": highway, "length": length,
                    "maxspeed": maxspeed, "oneway": bool(data.get('oneway', False)),
                    "trips": trips
                },
                "geometry": {"type": "LineString", "coordinates": coords}
            })

        return {
            "edges": {"type": "FeatureCollection", "features": features},
            "edge_count": len(features),
            "node_count": len(G.nodes),
            "total_assigned": total_assigned,
            "origin_node": origin_node
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}