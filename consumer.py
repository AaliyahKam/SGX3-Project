import requests

lat = 30.2895
lon = -97.7368

rush_hours = list(range(7, 10)) + list(range(16, 19))
all_rush_records = []

print("Requesting rush hour incidents from webservice...")

for hour in rush_hours:
    url = f"http://35.206.76.195:8052/ByHourRange?start_hour={hour}&end_hour={hour}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        matches = data.get("matching_incidents", [])
        print(f"Hour {hour}: {len(matches)} incidents")
        all_rush_records.extend(matches)
    else:
        print(f"Error fetching hour {hour}: Status {response.status_code}")

print(f"Total rush hour incidents: {len(all_rush_records)}")

incidents_2024 = [
    record for record in all_rush_records
    if 'Published Date' in record and '2024' in record['Published Date']
]

print(f"Incidents from 2024 only: {len(incidents_2024)}")

print("Checking for incidents near UT Austin (within 1 km)...")
nearby_url = f"http://35.206.76.195:8052/Nearby?lat={lat}&lon={lon}"
nearby_response = requests.get(nearby_url)

if nearby_response.status_code == 200:
    nearby_data = nearby_response.json().get("matching_incidents", [])

    combined_matches = []
    for inc in nearby_data:
        if any(
            inc['Published Date'] == i['Published Date'] and
            inc['Traffic Report ID'] == i['Traffic Report ID']
            for i in incidents_2024
        ):
            combined_matches.append(inc)

    print(f"Total incidents during rush hour in 2024 within 1 km of UT Austin: {len(combined_matches)}")

    if combined_matches:
        print("Sample incident:")
        print(combined_matches[0])
    else:
        print("No matching incidents found.")
else:
    print("Failed to retrieve nearby incidents.")

