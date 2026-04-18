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


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


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
def get_od_flows_tiered(oa: str, pin_lat: float, pin_lng: float):
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
                dest = item.get("place_of_work", {}).get("geogcode", "")
                count = item.get("obs_value", {}).get("value", 0)
                if count and count > 0 and dest.startswith('E02'):
                    flows.append({"destination": dest, "count": count})

        total = sum(f["count"] for f in flows)
        for f in flows:
            f["percentage"] = round((f["count"] / total * 100), 2) if total > 0 else 0
        flows.sort(key=lambda x: x["count"], reverse=True)

        if not flows:
            return {"origin_oa": oa, "total_trips": total, "flows": []}

        msoa_codes = [f["destination"] for f in flows[:50]]
        codes_str = "','".join(msoa_codes)

        centroid_url = (
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "MSOA_Dec_2011_PWC_in_England_and_Wales_2022/FeatureServer/0/query"
            f"?where=msoa11cd IN ('{codes_str}')"
            "&outFields=msoa11cd&returnGeometry=true&outSR=4326&f=json&resultRecordCount=100"
        )
        centroid_resp = requests.get(centroid_url, timeout=15)
        centroid_data = centroid_resp.json()

        centroid_map = {}
        if centroid_data.get('features'):
            for feat in centroid_data['features']:
                code = feat['attributes']['msoa11cd']
                geom = feat.get('geometry', {})
                if geom.get('x') and geom.get('y'):
                    centroid_map[code] = {'lat': geom['y'], 'lng': geom['x']}

        la_lookup_url = (
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            "FeatureServer/0/query"
            f"?where=MSOA11CD IN ('{codes_str}')"
            "&outFields=MSOA11CD,LAD11CD,LAD11NM&f=json&resultRecordCount=200"
        )
        la_resp = requests.get(la_lookup_url, timeout=15)
        la_data = la_resp.json()

        la_map = {}
        if la_data.get('features'):
            for feat in la_data['features']:
                attrs = feat['attributes']
                msoa = attrs.get('MSOA11CD')
                la_code = attrs.get('LAD11CD')
                la_name = attrs.get('LAD11NM')
                if msoa and la_code:
                    la_map[msoa] = {'code': la_code, 'name': la_name}

        tiered_flows = []
        la_aggregated = {}

        for f in flows[:50]:
            msoa = f["destination"]
            centroid = centroid_map.get(msoa)
            if not centroid:
                continue

            dist = haversine_km(pin_lat, pin_lng, centroid['lat'], centroid['lng'])

            if dist <= 10:
                tier = 'local'
                # Expand MSOA to individual OAs
                try:
                    oa_url = (
                        "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
                        "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
                        "FeatureServer/0/query"
                        f"?where=MSOA11CD='{msoa}'"
                        "&outFields=OA11CD&f=json&resultRecordCount=500"
                    )
                    oa_lookup_resp = requests.get(oa_url, timeout=10)
                    oa_lookup_data = oa_lookup_resp.json()
                    oa_codes = [feat['attributes']['OA11CD'] for feat in oa_lookup_data.get('features', [])]

                    if oa_codes:
                        codes_str = "','".join(oa_codes)
                        oa_centroid_url = (
                            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
                            "Output_Areas_Dec_2011_PWC_2022/FeatureServer/0/query"
                            f"?where=OA11CD IN ('{codes_str}')"
                            "&outFields=OA11CD&outSR=4326&returnGeometry=true&f=json&resultRecordCount=500"
                        )
                        oa_centroid_resp = requests.get(oa_centroid_url, timeout=10)
                        oa_centroid_data = oa_centroid_resp.json()
                        oa_count = len(oa_centroid_data.get('features', []))

                        for oa_feat in oa_centroid_data.get('features', []):
                            oa_code = oa_feat['attributes'].get('OA11CD')
                            geom = oa_feat.get('geometry', {})
                            if oa_code and geom.get('x') and geom.get('y'):
                                # Distribute MSOA trips equally across OAs
                                oa_count_val = round(f['count'] / oa_count, 2) if oa_count > 0 else 0
                                oa_pct = f['percentage'] / oa_count if oa_count > 0 else 0
                                tiered_flows.append({
                                    'destination': oa_code,
                                    'count': round(oa_count_val),
                                    'percentage': round(oa_pct, 4),
                                    'tier': 'local',
                                    'distance_km': round(dist, 1),
                                    'routing_lat': geom['y'],
                                    'routing_lng': geom['x'],
                                    'display_code': oa_code,
                                    'display_type': 'OA',
                                    'parent_msoa': msoa
                                })
                    else:
                        # Fallback to MSOA if no OAs found
                        tiered_flows.append({
                            **f,
                            'tier': tier,
                            'distance_km': round(dist, 1),
                            'routing_lat': centroid['lat'],
                            'routing_lng': centroid['lng'],
                            'display_code': msoa,
                            'display_type': 'MSOA'
                        })
                except:
                    tiered_flows.append({
                        **f,
                        'tier': tier,
                        'distance_km': round(dist, 1),
                        'routing_lat': centroid['lat'],
                        'routing_lng': centroid['lng'],
                        'display_code': msoa,
                        'display_type': 'MSOA'
                    })
            elif dist <= 50:
                tier = 'regional'
                tiered_flows.append({
                    **f,
                    'tier': tier,
                    'distance_km': round(dist, 1),
                    'routing_lat': centroid['lat'],
                    'routing_lng': centroid['lng'],
                    'display_code': msoa,
                    'display_type': 'MSOA'
                })
            else:
                la = la_map.get(msoa, {})
                la_code = la.get('code', 'unknown')
                if la_code not in la_aggregated:
                    la_aggregated[la_code] = {
                        'destination': la_code,
                        'la_name': la.get('name', la_code),
                        'count': 0,
                        'percentage': 0,
                        'tier': 'national',
                        'distance_km': round(dist, 1),
                        'routing_lat': centroid['lat'],
                        'routing_lng': centroid['lng'],
                        'display_code': la_code,
                        'display_type': 'LA'
                    }
                la_aggregated[la_code]['count'] += f['count']

        for la_flow in la_aggregated.values():
            la_flow['percentage'] = round(la_flow['count'] / total * 100, 2) if total > 0 else 0
            tiered_flows.append(la_flow)

        tiered_flows.sort(key=lambda x: x['count'], reverse=True)

        return {
            "origin_oa": oa,
            "total_trips": total,
            "flows": tiered_flows,
            "tiers": {
                "local": len([f for f in tiered_flows if f['tier'] == 'local']),
                "regional": len([f for f in tiered_flows if f['tier'] == 'regional']),
                "national": len([f for f in tiered_flows if f['tier'] == 'national'])
            }
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/oa-within-msoa")
def get_oa_within_msoa(msoa: str):
    try:
        lookup_url = (
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "OA11_LSOA11_MSOA11_LAD11_EW_LUv2_b3fe7c68f4b2420185eaff6284d4c125/"
            "FeatureServer/0/query"
            f"?where=MSOA11CD='{msoa}'"
            "&outFields=OA11CD&f=json&resultRecordCount=500"
        )
        lookup_resp = requests.get(lookup_url, timeout=15)
        lookup_data = lookup_resp.json()

        if not lookup_data.get('features'):
            return {"msoa": msoa, "oas": []}

        oa_codes = [f['attributes']['OA11CD'] for f in lookup_data['features']]
        codes_str = "','".join(oa_codes)

        centroid_url = (
            "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
            "Output_Areas_Dec_2011_PWC_2022/FeatureServer/0/query"
            f"?where=OA11CD IN ('{codes_str}')"
            "&outFields=OA11CD&outSR=4326&returnGeometry=true&f=json&resultRecordCount=500"
        )
        centroid_resp = requests.get(centroid_url, timeout=15)
        centroid_data = centroid_resp.json()

        oas = []
        if centroid_data.get('features'):
            for feat in centroid_data['features']:
                oa_code = feat['attributes'].get('OA11CD')
                geom = feat.get('geometry', {})
                if oa_code and geom.get('x') and geom.get('y'):
                    oas.append({
                        'oa': oa_code,
                        'lat': geom['y'],
                        'lng': geom['x']
                    })

        return {"msoa": msoa, "oa_count": len(oas), "oas": oas}

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


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
        "&outFields=OA11CD,LSOA11CD,LSOA11NM,MSOA11CD,MSOA11NM"
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
                "lsoa": attrs.get("LSOA11CD"),
                "lsoa_name": attrs.get("LSOA11NM"),
                "msoa": attrs.get("MSOA11CD"),
                "msoa_name": attrs.get("MSOA11NM")
            }
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
                "properties": {"name": name, "highway": highway, "length": length,
                               "maxspeed": maxspeed, "oneway": bool(row.get('oneway', False))},
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


@app.get("/api/split-edge")
def split_edge(pin_lat: float, pin_lng: float, access_lat: float, access_lng: float, radius_m: int = 1000):
    try:
        import osmnx as ox
        from shapely.geometry import Point, LineString

        G = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)
        access_point = Point(access_lng, access_lat)
        nearest = ox.nearest_edges(G, access_lng, access_lat)
        u, v, k = nearest
        edge_data = G[u][v][k]

        if 'geometry' in edge_data:
            line = edge_data['geometry']
        else:
            line = LineString([(G.nodes[u]['x'], G.nodes[u]['y']), (G.nodes[v]['x'], G.nodes[v]['y'])])

        nearest_pt = line.interpolate(line.project(access_point))
        return {"access_lat": nearest_pt.y, "access_lng": nearest_pt.x, "snapped": True, "edge": {"u": u, "v": v}}

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/assign-trips")
def assign_trips(pin_lat: float, pin_lng: float, radius_m: int = 1000, vehicle_trips: int = 0, flows: str = "", access_lat: float = None, access_lng: float = None):
    try:
        import osmnx as ox
        import networkx as nx
        from shapely.geometry import Point, LineString

        routing_radius = max(radius_m * 4, 15000)
        print(f"Loading routing network ({routing_radius}m) and display network ({radius_m}m)")

        G_route = ox.graph_from_point((pin_lat, pin_lng), dist=routing_radius, network_type='drive', simplify=True)
        G_route = ox.add_edge_speeds(G_route)
        G_route = ox.add_edge_travel_times(G_route)

        G_display = ox.graph_from_point((pin_lat, pin_lng), dist=radius_m, network_type='drive', simplify=True)

        if access_lat and access_lng:
            u, v, k = ox.nearest_edges(G_route, access_lng, access_lat)
            edge_data = G_route[u][v][k]

            if 'geometry' in edge_data:
                line = edge_data['geometry']
            else:
                line = LineString([(G_route.nodes[u]['x'], G_route.nodes[u]['y']),
                                   (G_route.nodes[v]['x'], G_route.nodes[v]['y'])])

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
            print(f"Using nearest node: {origin_node}")

        flow_list = []
        if flows:
            for item in flows.split(','):
                parts = item.strip().split(':')
                if len(parts) == 4:
                    try:
                        flow_list.append({
                            'msoa': parts[0].strip(),
                            'percentage': float(parts[1].strip()),
                            'dest_lat': float(parts[2].strip()),
                            'dest_lng': float(parts[3].strip())
                        })
                    except:
                        pass
                elif len(parts) == 2:
                    try:
                        flow_list.append({
                            'msoa': parts[0].strip(),
                            'percentage': float(parts[1].strip()),
                            'dest_lat': None,
                            'dest_lng': None
                        })
                    except:
                        pass

        edge_trips_route = {}
        for uu, vv, kk in G_route.edges(keys=True):
            edge_trips_route[(uu, vv, kk)] = 0

        total_assigned = 0

        for flow in flow_list:
            msoa = flow['msoa']
            pct = flow['percentage']
            trips_to_dest = round(vehicle_trips * pct / 100)
            if trips_to_dest == 0:
                continue

            try:
                dest_lat = flow.get('dest_lat')
                dest_lng = flow.get('dest_lng')

                if not dest_lat or not dest_lng:
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

                dest_node = ox.nearest_nodes(G_route, dest_lng, dest_lat)

                if dest_node == origin_node:
                    continue

                try:
                    path = nx.shortest_path(G_route, origin_node, dest_node, weight='travel_time')
                except nx.NetworkXNoPath:
                    try:
                        G_undirected = G_route.to_undirected()
                        path = nx.shortest_path(G_undirected, origin_node, dest_node, weight='travel_time')
                    except:
                        continue

                if len(path) < 2:
                    continue

                for i in range(len(path) - 1):
                    uu, vv = path[i], path[i + 1]
                    if G_route.has_edge(uu, vv):
                        kk = min(G_route[uu][vv].keys())
                        edge_trips_route[(uu, vv, kk)] = edge_trips_route.get((uu, vv, kk), 0) + trips_to_dest

                total_assigned += trips_to_dest
                print(f"Routed {trips_to_dest} trips to {msoa} via {len(path)} nodes")

            except Exception as e:
                print(f"Error routing to {msoa}: {e}")
                continue

        print(f"Total assigned: {total_assigned}")

        nodes_display, edges_display = ox.graph_to_gdfs(G_display)
        edges_reset = edges_display.reset_index()

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

            u_disp = row['u']
            v_disp = row['v']

            try:
                u_x = nodes_display.loc[u_disp, 'x'] if u_disp in nodes_display.index else None
                u_y = nodes_display.loc[u_disp, 'y'] if u_disp in nodes_display.index else None
                v_x = nodes_display.loc[v_disp, 'x'] if v_disp in nodes_display.index else None
                v_y = nodes_display.loc[v_disp, 'y'] if v_disp in nodes_display.index else None
            except:
                u_x = u_y = v_x = v_y = None

            trips = 0
            if u_x and v_x:
                u_route = ox.nearest_nodes(G_route, u_x, u_y)
                v_route = ox.nearest_nodes(G_route, v_x, v_y)
                if G_route.has_edge(u_route, v_route):
                    kk = min(G_route[u_route][v_route].keys())
                    trips = edge_trips_route.get((u_route, v_route, kk), 0)

            features.append({
                "type": "Feature",
                "properties": {
                    "name": name, "highway": highway, "length": length,
                    "maxspeed": maxspeed, "oneway": bool(row.get('oneway', False)),
                    "trips": trips
                },
                "geometry": {"type": "LineString", "coordinates": list(row['geometry'].coords)}
            })

        return {
            "edges": {"type": "FeatureCollection", "features": features},
            "edge_count": len(features),
            "node_count": len(G_display.nodes),
            "total_assigned": total_assigned,
            "origin_node": origin_node
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}