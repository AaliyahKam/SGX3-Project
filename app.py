from flask import Flask, jsonify, request
import pandas as pd
import os
from io import StringIO
from geopy.distance import geodesic

# Define your global DataFrame
traffic_df = None

app = Flask(__name__)

def load_traffic_data():
    global traffic_df
    print("Loading Austin Traffic Data...")
    traffic_df = pd.read_csv(os.path.join("atxtraffic.csv"))
    print(f"Loaded {len(traffic_df)} rows into memory.")

@app.route("/")
def index():
    global traffic_df
    sample = traffic_df.head(10).to_dict(orient="records")
    return jsonify(sample)

@app.route("/head")
def top():
    global traffic_df
    num = int(request.args.get('count'))
    sample = traffic_df.head(num).to_dict(orient="records")
    return jsonify(sample)

# NEW: /shape endpoint
@app.route("/shape")
def shape():
    global traffic_df
    rows, cols = traffic_df.shape
    return jsonify({"rows": rows, "columns": cols})

# NEW: /columns endpoint
@app.route("/columns")
def columns():
    global traffic_df
    return jsonify({"columns": list(traffic_df.columns)})

# NEW: /info endpoint
@app.route("/info")
def info():
    global traffic_df
    buffer = StringIO()
    traffic_df.info(buf=buffer)
    info_str = buffer.getvalue()
    return jsonify({"info": info_str})

# NEW: /describe endpoint
@app.route("/describe")
def describe():
    global traffic_df
    desc = traffic_df.describe().to_dict()
    return jsonify(desc)

@app.route("/UniqueValues")
def unique_values():
    global traffic_df

    # Get the column name from the query string
    column = request.args.get('ColumnName')

    # Handle missing or invalid column name
    if not column:
        return jsonify({"error": "Please provide a ColumnName parameter."}), 400

    if column not in traffic_df.columns:
        return jsonify({"error": f"Column '{column}' not found in the dataset."}), 404

    # Get unique values (excluding NaNs)
    unique_vals = traffic_df[column].dropna().unique().tolist()
    unique_count = len(unique_vals)

    # Return as JSON
    return jsonify({
        "column": column,
        "unique_count": unique_count,
        "unique_values": unique_vals
    })


@app.route("/FilterByValueAndYear")
def filter_by_value_and_year():
    global traffic_df

    # Get query parameters
    col_name = request.args.get("ColumnName")
    col_value = request.args.get("ColumnValue")
    year = request.args.get("Year")

    # Validate input
    if not col_name or not col_value or not year:
        return jsonify({"error": "Missing one or more required parameters: ColumnName, ColumnValue, Year"}), 400

    if col_name not in traffic_df.columns:
        return jsonify({"error": f"Column '{col_name}' not found in dataset."}), 404

    try:
        year = int(year)
    except ValueError:
        return jsonify({"error": "Year must be a valid number."}), 400

    # Ensure Published Date is in datetime format
    traffic_df['Published Date'] = pd.to_datetime(traffic_df['Published Date'], errors='coerce')

    # Filter the data
    filtered = traffic_df[
        (traffic_df[col_name] == col_value) &
        (traffic_df['Published Date'].dt.year == year)
    ]

    # Return matching incidents as JSON
    return jsonify({
        "column": col_name,
        "value": col_value,
        "year": year,
        "match_count": len(filtered),
        "matching_incidents": filtered.head(10).to_dict(orient="records")  # limit to first 10 for performance
    })






@app.route("/ByHourRange")
def by_hour_range():
    global traffic_df

    try:
        start_hour = int(request.args.get("start_hour"))
        end_hour = int(request.args.get("end_hour"))
    except (TypeError, ValueError):
        return jsonify({"error": "start_hour and end_hour must be integers."}), 400

    if not (0 <= start_hour <= 23) or not (0 <= end_hour <= 23):
        return jsonify({"error": "Hour values must be between 0 and 23."}), 400

    # Ensure datetime is parsed
    traffic_df['Published Date'] = pd.to_datetime(traffic_df['Published Date'], errors='coerce')

    # Extract hour if it doesn't already exist
    if 'hour' not in traffic_df.columns:
        traffic_df['hour'] = traffic_df['Published Date'].dt.hour

    # Filter by hour range
    filtered = traffic_df[(traffic_df['hour'] >= start_hour) & (traffic_df['hour'] <= end_hour)]

    return jsonify({
        "start_hour": start_hour,
        "end_hour": end_hour,
        "match_count": len(filtered),
        "matching_incidents": filtered.head(10).to_dict(orient="records")  # limit for performance
    })



@app.route("/Nearby")
def nearby():
    global traffic_df

    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Latitude and Longitude must be valid floats."}), 400

    # Column names may vary; use correct ones from your CSV
    if "Latitude" not in traffic_df.columns or "Longitude" not in traffic_df.columns:
        return jsonify({"error": "Dataset must have 'Latitude' and 'Longitude' columns."}), 500

    def is_within_1km(row):
        try:
            point = (row['Latitude'], row['Longitude'])
            return geodesic((lat, lon), point).km <= 1
        except:
            return False

    nearby_records = traffic_df[traffic_df.apply(is_within_1km, axis=1)]

    return jsonify({
        "latitude": lat,
        "longitude": lon,
        "match_count": len(nearby_records),
        "matching_incidents": nearby_records.head(10).to_dict(orient="records")  # limit results
    })





@app.route("/RushHourUT")
def rush_hour_ut():
    lat = 30.2895
    lon = -97.7368
    rush_hours = list(range(7, 10)) + list(range(16, 19))

    traffic_df['Published Date'] = pd.to_datetime(traffic_df['Published Date'], errors='coerce')
    traffic_df['hour'] = traffic_df['Published Date'].dt.hour
    traffic_df['year'] = traffic_df['Published Date'].dt.year

    rush_df = traffic_df[
        ((traffic_df['hour'] >= 7) & (traffic_df['hour'] <= 9)) |
        ((traffic_df['hour'] >= 16) & (traffic_df['hour'] <= 18))
    ]

    rush_df = rush_df[rush_df['year'] == 2024]

    from geopy.distance import geodesic
    def within_1km(row):
        try:
            return geodesic((lat, lon), (row['Latitude'], row['Longitude'])).km <= 1
        except:
            return False

    result = rush_df[rush_df.apply(within_1km, axis=1)]

    return jsonify({
        "total_matches": len(result),
        "sample": result.head(5).to_dict(orient="records")
        })






if __name__ == "__main__":
    load_traffic_data()  # <- This runs BEFORE the server starts
    app.run(debug=True, host="0.0.0.0", port=8052)
