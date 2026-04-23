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
HERE_API_KEY = os.getenv("HERE_API_KEY")


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, list):
        cleaned = [clean_value(i) for i in v]
        return cleaned[0] if len(cleaned) == 1 else cleaned
    return v


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def here_route(origin_lat, origin_lng, dest_lat, dest_lng, departure_time_str):
    """Call HERE Routing API v8 and return a list of (lat, lng) coordinates."""
    try:
        from datetime import datetime, timedelta
        today = datetime.utcnow()
        days_ahead = 0 - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        next_monday = today + timedelta(days=days_ahead)
        hour, minute = map(int, departure_time_str.split(':'))
        departure_dt = next_monday.replace(hour=hour, minute=minute, second=0, microsecond=0)
        departure_iso = departure_dt.strftime('%Y-%m-%dT%H:%M:%S')

        params = {
            "apikey": HERE_API_KEY,
            "transportMode": "car",
            "origin": f"{origin_lat},{origin_lng}",
            "destination": f"{dest_lat},{dest_lng}",
            "return": "polyline",
            "departureTime": departure_iso,
            "routingMode": "fast",
        }
        resp = requests.get("https://router.hereapi.com/v8/routes", params=params, timeout=15)
        data = resp.json()

        if "routes" not in data or not data["routes"]:
            print(f"HERE no route response: {data}")
            return None

        encoded = data["routes"][0]["sections"][0]["polyline"]
        return decode_here_polyline(encoded)

    except Exception as e:
        print(f"HERE routing error: {e}")
        return None


def decode_here_polyline(encoded):
    """Decode HERE flexible polyline encoding to list of (lat, lng) tuples."""
    DECODING_TABLE = [
        62, -1, -1, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, -1, -1, -1, -1, 63, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
    ]

    def decode_unsigned(encoded, i):
        result, shift = 0, 0
        while True:
            c = DECODING_TABLE[ord(encoded[i]) - 45]
            i += 1
            result |= (c & 0x1F) << shift
            shift += 5
            if c < 0x20:
                break
        return result, i

    def decode_signed(encoded, i):
        unsigned, i = decode_unsigned(encoded, i)
        result = ~(unsigned >> 1) if unsigned & 1 else unsigned >> 1
        return result, i

    i = 0
    _, i = decode_unsigned(encoded, i)
    header_content, i = decode_unsigned(encoded, i)
    precision = header_content & 0xF
    third_dim = (header_content >> 4) & 0x7
    factor = 10 ** precision
    coords, lat, lng = [], 0, 0

    while i < len(encoded):
        dlat, i = decode_signed(encoded, i)
        dlng, i = decode_signed(encoded, i)
        if third_dim:
            _, i = decode_signed(encoded, i)
        lat += dlat
        lng += dlng
        coords.append((lat / factor, lng / factor))

    return coords


def clip_polyline_to_polygon(coords, polygon_boundary):
    """Clip (lat, lng) coords to those within the polygon boundary (with buffer)."""
    if not polygon_boundary or not coords:
        return coords
    from shapely.geometry import Point as ShapelyPoint
    filter_poly = polygon_boundary.buffer(0.003)
    clipped = [(lat, lng) for lat, lng in coords if filter_poly.contains(ShapelyPoint(lng, lat))]
    return clipped if len(clipped) >= 2 else coords[:2]


@app.get("/")
def root():
    return {"status": "ODIN backend running"}


@app.get("/api/od-flows")
def get_od_flows(oa: str):
    url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_1228_1.data.json"
    params = {
        "date": "latest", "currently_residing_in": oa, "place_of_work": "TYPE297",
        "measures": "20100", "uid": NOMIS_API_KEY,
        "select": "currently_residing_in_code,place_of_work_code,obs_value",
    }
    try:
        data = requests.get(url, params=params, timeout=30).json()
        flows = []
        if "obs" in data:
            for item in data["obs"]:
                dest = item.get("place_of_work", {}).get("geogcode", "")
                count = item.get("obs_value", {}).get("value", 0)
                if count and count > 0:
                    flows.append({"destination": dest, "count": count})
        total = sum(f["count"] for f in flows)
        for f in flows:
            f["percentage"] = round((f["count"] / total * 100), 2) if total > 0 else 0
        flows.sort(key=lambda x: x["count"], reverse=True)
        return {"origin_oa": oa, "total_trips": total, "flows": flows}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/od-flows-tiered")
def get_od_flows_tiered(oa: str):
    try:
        lookup_data = requests.get(
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            "FeatureServer/0/query?where=" + f"OA11CD='{oa}'" +
            "&outFields=OA11CD,LSOA11CD,MSOA11CD,LAD11CD,LAD11NM&f=json&resultRecordCount=1",
            timeout=15).json()

        if not lookup_data.get("features"):
            return {"error": "Could not look up origin OA"}

        attrs = lookup_data["features"][0]["attributes"]
        origin_lad = attrs.get("LAD11CD")
        origin_lad_name = attrs.get("LAD11NM")
        origin_lsoa = attrs.get("LSOA11CD")
        origin_msoa = attrs.get("MSOA11CD")

        lsoa_data = requests.get("https://www.nomisweb.co.uk/api/v01/dataset/NM_1228_1.data.json", params={
            "date": "latest", "currently_residing_in": oa, "place_of_work": "TYPE298",
            "measures": "20100", "uid": NOMIS_API_KEY,
            "select": "place_of_work_code,place_of_work_name,obs_value", "ExcludeMissingValues": "true",
        }, timeout=60).json()

        local_lsoas = set(f["attributes"]["LSOA11CD"] for f in requests.get(
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            f"FeatureServer/0/query?where=LAD11CD='{origin_lad}'"
            "&outFields=LSOA11CD&returnDistinctValues=true&f=json&resultRecordCount=2000",
            timeout=15).json().get("features", []))

        msoa_data = requests.get("https://www.nomisweb.co.uk/api/v01/dataset/NM_1228_1.data.json", params={
            "date": "latest", "currently_residing_in": oa, "place_of_work": "TYPE297",
            "measures": "20100", "uid": NOMIS_API_KEY,
            "select": "place_of_work_code,place_of_work_name,obs_value", "ExcludeMissingValues": "true",
        }, timeout=60).json()

        local_msoas = set(f["attributes"]["MSOA11CD"] for f in requests.get(
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            f"FeatureServer/0/query?where=LAD11CD='{origin_lad}'"
            "&outFields=MSOA11CD&returnDistinctValues=true&f=json&resultRecordCount=500",
            timeout=15).json().get("features", []))

        flows, total_trips = [], 0

        for item in lsoa_data.get("obs", []):
            dest_code = item.get("place_of_work", {}).get("geogcode", "")
            dest_name = item.get("place_of_work", {}).get("description", "")
            count = item.get("obs_value", {}).get("value", 0) or 0
            if count > 0 and dest_code in local_lsoas:
                flows.append({"destination": dest_code, "name": dest_name, "count": count, "type": "lsoa"})
                total_trips += count

        for item in msoa_data.get("obs", []):
            dest_code = item.get("place_of_work", {}).get("geogcode", "")
            dest_name = item.get("place_of_work", {}).get("description", "")
            count = item.get("obs_value", {}).get("value", 0) or 0
            if count > 0 and dest_code not in local_msoas:
                flows.append({"destination": dest_code, "name": dest_name, "count": count, "type": "msoa"})
                total_trips += count

        for f in flows:
            f["percentage"] = round((f["count"] / total_trips * 100), 2) if total_trips > 0 else 0
        flows.sort(key=lambda x: x["count"], reverse=True)

        return {
            "origin_oa": oa, "origin_lsoa": origin_lsoa, "origin_msoa": origin_msoa,
            "origin_lad": origin_lad, "origin_lad_name": origin_lad_name,
            "total_trips": total_trips, "flows": flows,
            "lsoa_count": sum(1 for f in flows if f["type"] == "lsoa"),
            "msoa_count": sum(1 for f in flows if f["type"] == "msoa"),
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/oa-within-msoa")
def get_oa_within_msoa(msoa: str):
    try:
        lookup_data = requests.get(
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            f"FeatureServer/0/query?where=MSOA11CD='{msoa}'"
            "&outFields=OA11CD&f=json&resultRecordCount=500", timeout=15).json()
        if not lookup_data.get('features'):
            return {"msoa": msoa, "oas": []}
        oa_codes = [f['attributes']['OA11CD'] for f in lookup_data['features']]
        codes_str = "','".join(oa_codes)
        centroid_data = requests.get(
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            f"Output_Areas_Dec_2011_PWC_2022/FeatureServer/0/query?where=OA11CD IN ('{codes_str}')"
            "&outFields=OA11CD&outSR=4326&returnGeometry=true&f=json&resultRecordCount=500", timeout=15).json()
        oas = []
        for feat in centroid_data.get('features', []):
            oa_code = feat['attributes'].get('OA11CD')
            geom = feat.get('geometry', {})
            if oa_code and geom.get('x') and geom.get('y'):
                oas.append({'oa': oa_code, 'lat': geom['y'], 'lng': geom['x']})
        return {"msoa": msoa, "oa_count": len(oas), "oas": oas}
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/mode-share")
def get_mode_share(msoa: str):
    url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_1208_1.data.json"
    params = {
        "date": "latest", "usual_residence": msoa, "place_of_work": "TYPE297",
        "transport_powpew11": "0,1,2,3,4,5,6,7,8,9,10,11", "measures": "20100",
        "uid": NOMIS_API_KEY, "select": "transport_powpew11_code,transport_powpew11_name,obs_value",
    }
    try:
        data = requests.get(url, params=params, timeout=30).json()
        mode_names = {
            0: "All", 1: "Work from home", 2: "Metro/tram", 3: "Train", 4: "Bus",
            5: "Taxi", 6: "Motorcycle", 7: "Car driver", 8: "Car passenger",
            9: "Bicycle", 10: "On foot", 11: "Other"
        }
        mode_totals, total = {}, 0
        for item in data.get("obs", []):
            code = item.get("transport_powpew11", {}).get("value", -1)
            count = item.get("obs_value", {}).get("value", 0) or 0
            if code == 0:
                total += count
            elif code in range(1, 12):
                mode_totals[code] = mode_totals.get(code, 0) + count
        if total == 0:
            total = sum(mode_totals.values())
        modes = [{"code": c, "name": mode_names.get(c, "Unknown"), "count": mode_totals[c],
                  "percentage": round(mode_totals[c] / total * 100, 1) if total > 0 else 0}
                 for c in sorted(mode_totals)]
        return {"msoa": msoa, "total": total, "modes": modes}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/oa-to-msoa")
def get_oa_to_msoa(oa: str):
    url = (
        "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
        "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
        "FeatureServer/0/query?where=" + f"OA11CD='{oa}'" +
        "&outFields=OA11CD,LSOA11CD,LSOA11NM,MSOA11CD,MSOA11NM&f=json&resultRecordCount=1"
    )
    try:
        data = requests.get(url, timeout=15).json()
        if "features" in data and data["features"]:
            attrs = data["features"][0]["attributes"]
            return {"oa": oa, "lsoa": attrs.get("LSOA11CD"), "lsoa_name": attrs.get("LSOA11NM"),
                    "msoa": attrs.get("MSOA11CD"), "msoa_name": attrs.get("MSOA11NM")}
        return {"oa": oa, "msoa": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/road-network")
def get_road_network(pin_lat: float, pin_lng: float, polygon: str = None, radius_m: int = 1000):
    try:
        import osmnx as ox
        from shapely.geometry import Polygon as ShapelyPolygon
        import json

        if polygon:
            coords = json.loads(polygon)
            G = ox.graph_from_polygon(ShapelyPolygon([(p[1], p[0]) for p in coords]).buffer(0.002),
                                      network_type='drive', simplify=True)
        else:
            G = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)

        nodes, edges = ox.graph_to_gdfs(G)
        features = []
        for _, row in edges.reset_index().iterrows():
            length = row.get('length', 0)
            try:
                length = float(length)
                if math.isnan(length):
                    length = 0
            except:
                length = 0
            features.append({
                "type": "Feature",
                "properties": {"name": clean_value(row.get('name')), "highway": clean_value(row.get('highway')),
                               "length": length, "maxspeed": clean_value(row.get('maxspeed')),
                               "oneway": bool(row.get('oneway', False))},
                "geometry": {"type": "LineString", "coordinates": list(row['geometry'].coords)}
            })

        node_features = [{"type": "Feature", "properties": {"osmid": int(row['osmid'])},
                          "geometry": {"type": "Point", "coordinates": [row['geometry'].x, row['geometry'].y]}}
                         for _, row in nodes.reset_index().iterrows()]

        return {"edges": {"type": "FeatureCollection", "features": features},
                "nodes": {"type": "FeatureCollection", "features": node_features},
                "edge_count": len(features), "node_count": len(node_features)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/split-edge")
def split_edge(pin_lat: float, pin_lng: float, access_lat: float, access_lng: float, polygon: str = None, radius_m: int = 1000):
    try:
        import osmnx as ox
        from shapely.geometry import Point, LineString, Polygon as ShapelyPolygon
        import json

        if polygon:
            coords = json.loads(polygon)
            G = ox.graph_from_polygon(ShapelyPolygon([(p[1], p[0]) for p in coords]).buffer(0.002),
                                      network_type='drive', simplify=True)
        else:
            G = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)

        access_point = Point(access_lng, access_lat)
        u, v, k = ox.nearest_edges(G, access_lng, access_lat)
        edge_data = G[u][v][k]
        line = edge_data.get('geometry', LineString([(G.nodes[u]['x'], G.nodes[u]['y']),
                                                      (G.nodes[v]['x'], G.nodes[v]['y'])]))
        nearest_pt = line.interpolate(line.project(access_point))
        return {"access_lat": nearest_pt.y, "access_lng": nearest_pt.x, "snapped": True, "edge": {"u": u, "v": v}}
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/assign-trips")
def assign_trips(
    pin_lat: float, pin_lng: float,
    radius_m: int = 1000,
    polygon: str = None,
    vehicle_trips: int = 0,
    arr_vehicle_trips: int = 0,
    dep_vehicle_trips: int = 0,
    flows: str = "",
    access_lat: float = None,
    access_lng: float = None,
    am_peak: str = "08:00",
    pm_peak: str = "17:00",
    assign_period: str = "am"
):
    try:
        import osmnx as ox
        import networkx as nx
        from shapely.geometry import Point, LineString, Polygon as ShapelyPolygon
        import json

        # Build display network
        shapely_poly_buffered = None
        if polygon:
            coords = json.loads(polygon)
            shapely_poly = ShapelyPolygon([(p[1], p[0]) for p in coords])
            shapely_poly_buffered = shapely_poly.buffer(0.002)
            G_display = ox.graph_from_polygon(shapely_poly_buffered, network_type='drive', simplify=True)
            print(f"Display network loaded from polygon ({len(G_display.nodes)} nodes)")
        else:
            G_display = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)
            print(f"Display network loaded from radius {radius_m}m")

        # Small NetworkX fallback network
        routing_radius = min(2000 if polygon else radius_m * 2, 3000)
        G_route = ox.graph_from_point((pin_lat, pin_lng), dist=routing_radius, network_type='drive', simplify=True)
        G_route = ox.add_edge_speeds(G_route)
        G_route = ox.add_edge_travel_times(G_route)
        print(f"Routing network loaded ({routing_radius}m fallback)")

        # Set up origin node
        if access_lat and access_lng:
            u, v, k = ox.nearest_edges(G_route, access_lng, access_lat)
            edge_data = G_route[u][v][k]
            line = edge_data.get('geometry', LineString([
                (G_route.nodes[u]['x'], G_route.nodes[u]['y']),
                (G_route.nodes[v]['x'], G_route.nodes[v]['y'])
            ]))
            snapped = line.interpolate(line.project(Point(access_lng, access_lat)))
            new_lng, new_lat = snapped.x, snapped.y
            new_node_id = 999999999
            G_route.add_node(new_node_id, x=new_lng, y=new_lat)
            speed = edge_data.get('speed_kph', 50)
            if not speed or (isinstance(speed, float) and math.isnan(speed)):
                speed = 50

            def dist_m(lng1, lat1, lng2, lat2):
                return ((lng2 - lng1) ** 2 + (lat2 - lat1) ** 2) ** 0.5 * 111320

            dist_u = dist_m(new_lng, new_lat, G_route.nodes[u]['x'], G_route.nodes[u]['y'])
            dist_v = dist_m(new_lng, new_lat, G_route.nodes[v]['x'], G_route.nodes[v]['y'])
            tt_u = dist_u / (speed * 1000 / 3600)
            tt_v = dist_v / (speed * 1000 / 3600)
            G_route.add_edge(new_node_id, u, 0, travel_time=tt_u, length=dist_u,
                geometry=LineString([(new_lng, new_lat), (G_route.nodes[u]['x'], G_route.nodes[u]['y'])]))
            G_route.add_edge(u, new_node_id, 0, travel_time=tt_u, length=dist_u,
                geometry=LineString([(G_route.nodes[u]['x'], G_route.nodes[u]['y']), (new_lng, new_lat)]))
            G_route.add_edge(new_node_id, v, 0, travel_time=tt_v, length=dist_v,
                geometry=LineString([(new_lng, new_lat), (G_route.nodes[v]['x'], G_route.nodes[v]['y'])]))
            G_route.add_edge(v, new_node_id, 0, travel_time=tt_v, length=dist_v,
                geometry=LineString([(G_route.nodes[v]['x'], G_route.nodes[v]['y']), (new_lng, new_lat)]))
            origin_node = new_node_id
            print(f"Access node at ({new_lat:.5f},{new_lng:.5f}), connected to {u} and {v}")
        else:
            origin_node = ox.nearest_nodes(G_route, pin_lng, pin_lat)

        origin_lat_coord = access_lat if access_lat else pin_lat
        origin_lng_coord = access_lng if access_lng else pin_lng

        # Parse flows
        flow_list = []
        if flows:
            for item in flows.split(','):
                parts = item.strip().split(':')
                if len(parts) == 2:
                    try:
                        flow_list.append({'msoa': parts[0].strip(), 'percentage': float(parts[1].strip()),
                                          'dest_lat': None, 'dest_lng': None})
                    except:
                        pass

        total_assigned = 0
        route_features = []  # HERE polyline features for direct display

        # Use split arr/dep if provided, otherwise fall back to single vehicle_trips total
        use_split = (arr_vehicle_trips > 0 or dep_vehicle_trips > 0)
        departure_time = am_peak if assign_period != 'pm' else pm_peak

        def route_one(msoa, pct, trips, origin_lat, origin_lng, dest_lat, dest_lng, direction):
            """Route trips in one direction and append to route_features. Returns trips assigned."""
            if trips == 0 or pct == 0:
                return 0
            try:
                if HERE_API_KEY:
                    here_coords = here_route(origin_lat, origin_lng, dest_lat, dest_lng, departure_time)
                    if here_coords:
                        clipped = clip_polyline_to_polygon(here_coords, shapely_poly_buffered)
                        if len(clipped) >= 2:
                            route_features.append({
                                "type": "Feature",
                                "properties": {"trips": trips, "destination": msoa, "direction": direction},
                                "geometry": {"type": "LineString",
                                             "coordinates": [[lng, lat] for lat, lng in clipped]}
                            })
                        print(f"HERE {direction} {trips} trips to {msoa} ({len(clipped)} coords)")
                        return trips

                # NetworkX fallback
                dest_node = ox.nearest_nodes(G_route, dest_lng, dest_lat)
                if dest_node == origin_node:
                    return 0
                try:
                    path = nx.shortest_path(G_route, origin_node, dest_node, weight='travel_time')
                except nx.NetworkXNoPath:
                    try:
                        path = nx.shortest_path(G_route.to_undirected(), origin_node, dest_node, weight='travel_time')
                    except:
                        return 0
                if len(path) < 2:
                    return 0
                path_coords = [[G_route.nodes[n]['x'], G_route.nodes[n]['y']] for n in path]
                route_features.append({
                    "type": "Feature",
                    "properties": {"trips": trips, "destination": msoa, "direction": direction},
                    "geometry": {"type": "LineString", "coordinates": path_coords}
                })
                print(f"NetworkX {direction} {trips} trips to {msoa} via {len(path)} nodes")
                return trips
            except Exception as e:
                print(f"Error routing {direction} to {msoa}: {e}")
                return 0

        for flow in flow_list:
            msoa = flow['msoa']
            pct = flow['percentage']
            if pct == 0:
                continue

            try:
                dest_lat = flow.get('dest_lat')
                dest_lng = flow.get('dest_lng')

                if not dest_lat or not dest_lng:
                    zone_code = msoa
                    if zone_code.startswith('E02') or zone_code.startswith('W02'):
                        centroid_url = (
                            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
                            "MSOA_Dec_2011_PWC_in_England_and_Wales_2022/FeatureServer/0/query"
                            f"?where=msoa11cd='{zone_code}'&outFields=msoa11cd&returnGeometry=true&outSR=4326&f=json&resultRecordCount=1"
                        )
                    else:
                        centroid_url = (
                            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
                            "LSOA_Dec_2011_PWC_in_England_and_Wales_2022/FeatureServer/0/query"
                            f"?where=LSOA11CD='{zone_code}'&outFields=LSOA11CD&returnGeometry=true&outSR=4326&f=json&resultRecordCount=1"
                        )
                    centroid_data = requests.get(centroid_url, timeout=10).json()
                    if not centroid_data.get('features'):
                        print(f"No centroid found for {zone_code}")
                        continue
                    geom = centroid_data['features'][0].get('geometry', {})
                    dest_lng = geom.get('x')
                    dest_lat = geom.get('y')
                    if not dest_lat or not dest_lng:
                        continue

                if use_split:
                    # Arrivals: destination → site
                    arr_trips = max(1, round(arr_vehicle_trips * pct / 100)) if arr_vehicle_trips > 0 else 0
                    total_assigned += route_one(msoa, pct, arr_trips, dest_lat, dest_lng, origin_lat_coord, origin_lng_coord, 'arrivals')
                    # Departures: site → destination
                    dep_trips = max(1, round(dep_vehicle_trips * pct / 100)) if dep_vehicle_trips > 0 else 0
                    total_assigned += route_one(msoa, pct, dep_trips, origin_lat_coord, origin_lng_coord, dest_lat, dest_lng, 'departures')
                else:
                    # Legacy: all trips site → destination
                    trips_to_dest = max(1, round(vehicle_trips * pct / 100))
                    total_assigned += route_one(msoa, pct, trips_to_dest, origin_lat_coord, origin_lng_coord, dest_lat, dest_lng, 'departures')

            except Exception as e:
                print(f"Error routing to {msoa}: {e}")
                continue

        print(f"Total assigned: {total_assigned}")

        # Build base road network for display context
        base_features = []
        for _, row in ox.graph_to_gdfs(G_display)[1].reset_index().iterrows():
            length = row.get('length', 0)
            try:
                length = float(length)
                if math.isnan(length):
                    length = 0
            except:
                length = 0
            base_features.append({
                "type": "Feature",
                "properties": {"name": clean_value(row.get('name')), "highway": clean_value(row.get('highway')),
                               "length": length, "maxspeed": clean_value(row.get('maxspeed')),
                               "oneway": bool(row.get('oneway', False)), "trips": 0},
                "geometry": {"type": "LineString", "coordinates": list(row['geometry'].coords)}
            })

        arr_assigned = sum(f["properties"]["trips"] for f in route_features if f["properties"].get("direction") == "arrivals")
        dep_assigned = sum(f["properties"]["trips"] for f in route_features if f["properties"].get("direction") == "departures")

        return {
            "edges": {"type": "FeatureCollection", "features": base_features},
            "routes": {"type": "FeatureCollection", "features": route_features},
            "edge_count": len(base_features),
            "route_count": len(route_features),
            "node_count": len(G_display.nodes),
            "total_assigned": total_assigned,
            "arr_assigned": arr_assigned,
            "dep_assigned": dep_assigned,
            "origin_node": origin_node
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}