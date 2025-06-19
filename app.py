from flask import Flask, jsonify, request
import pandas as pd
import os
from io import StringIO


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




if __name__ == "__main__":
    load_traffic_data()  # <- This runs BEFORE the server starts
    app.run(debug=True, host="0.0.0.0", port=8052)
