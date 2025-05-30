


# import json
# import random
# import boto3
# from datetime import datetime, timedelta

# # AWS S3 configuration
# s3 = boto3.client("s3")
# bucket_name = "real-estate-datalake"
# current_year = datetime.now().strftime("%Y")

# # Real Berlin locations with latitude, longitude, and price multipliers
# locations = {
#     "Unter den Linden, Mitte": {"lat": 52.5163, "lon": 13.3880, "price_multiplier": 1.25, "median_income": 60000, "density": 4500, "crime_rate": 4.5},
#     "Kurfürstendamm, Charlottenburg": {"lat": 52.5030, "lon": 13.3300, "price_multiplier": 1.20, "median_income": 55000, "density": 3800, "crime_rate": 4.0},
#     "Oranienstraße, Kreuzberg": {"lat": 52.5000, "lon": 13.4170, "price_multiplier": 1.15, "median_income": 42000, "density": 6200, "crime_rate": 7.0},
#     "Kollwitzstraße, Prenzlauer Berg": {"lat": 52.5370, "lon": 13.4160, "price_multiplier": 1.22, "median_income": 53000, "density": 4600, "crime_rate": 4.8},
#     "Karl-Marx-Allee, Friedrichshain": {"lat": 52.5150, "lon": 13.4440, "price_multiplier": 1.10, "median_income": 47000, "density": 5000, "crime_rate": 6.0},
#     "Hermannstraße, Neukölln": {"lat": 52.4800, "lon": 13.4250, "price_multiplier": 0.95, "median_income": 39000, "density": 5800, "crime_rate": 6.5},
#     "Schlossstraße, Steglitz": {"lat": 52.4550, "lon": 13.3150, "price_multiplier": 1.05, "median_income": 48000, "density": 3200, "crime_rate": 4.2},
#     "Alt-Kaulsdorf Straße, Marzahn": {"lat": 52.5500, "lon": 13.5800, "price_multiplier": 0.85, "median_income": 35000, "density": 3000, "crime_rate": 5.0},
#     "Klosterstraße, Zehlendorf": {"lat": 52.4300, "lon": 13.2500, "price_multiplier": 1.18, "median_income": 57000, "density": 2800, "crime_rate": 3.8},
#     "Spandauer Damm, Spandau": {"lat": 52.5400, "lon": 13.2000, "price_multiplier": 0.90, "median_income": 40000, "density": 2900, "crime_rate": 4.5},
#     "Potsdamer Straße, Schöneberg": {"lat": 52.4950, "lon": 13.3650, "price_multiplier": 1.15, "median_income": 50000, "density": 4000, "crime_rate": 5.0},
#     "Frankfurter Allee, Lichtenberg": {"lat": 52.5150, "lon": 13.4700, "price_multiplier": 0.95, "median_income": 41000, "density": 4500, "crime_rate": 5.5},
#     "Rathausstraße, Pankow": {"lat": 52.5600, "lon": 13.4000, "price_multiplier": 1.10, "median_income": 46000, "density": 3400, "crime_rate": 4.3},
#     "Tempelhofer Damm, Tempelhof": {"lat": 52.4700, "lon": 13.4000, "price_multiplier": 1.00, "median_income": 45000, "density": 3700, "crime_rate": 4.7},
#     "Havelchaussee, Wannsee": {"lat": 52.4200, "lon": 13.1500, "price_multiplier": 1.20, "median_income": 58000, "density": 2500, "crime_rate": 3.5},
#     "Biesdorfer Straße, Hellersdorf": {"lat": 52.5300, "lon": 13.6100, "price_multiplier": 0.88, "median_income": 36000, "density": 3100, "crime_rate": 5.2},
#     "Reinickendorfer Straße, Wedding": {"lat": 52.5400, "lon": 13.3600, "price_multiplier": 0.98, "median_income": 40000, "density": 4800, "crime_rate": 6.0},
#     "Treptower Straße, Treptow": {"lat": 52.4900, "lon": 13.4500, "price_multiplier": 1.05, "median_income": 43000, "density": 4200, "crime_rate": 5.5},
#     "Alt-Tegel Straße, Tegel": {"lat": 52.5800, "lon": 13.2700, "price_multiplier": 0.92, "median_income": 41000, "density": 3000, "crime_rate": 4.4},
#     "Wuhletalstraße, Köpenick": {"lat": 52.4600, "lon": 13.5800, "price_multiplier": 0.97, "median_income": 42000, "density": 3400, "crime_rate": 4.8}
# }

# # Property type distribution and characteristics
# property_types = {
#     "apartment": {"probability": 0.6, "size_range": (30, 120), "lot_size_range": (0, 0), "bedrooms_range": (1, 3)},
#     "single_family_house": {"probability": 0.2, "size_range": (100, 250), "lot_size_range": (200, 500), "bedrooms_range": (3, 5)},
#     "multi_family_house": {"probability": 0.15, "size_range": (150, 400), "lot_size_range": (300, 600), "bedrooms_range": (4, 8)},
#     "commercial": {"probability": 0.05, "size_range": (50, 300), "lot_size_range": (100, 400), "bedrooms_range": (0, 0)}
# }

# price_trends = {
#     2003: 1500, 2004: 1550, 2005: 1600, 2006: 1650, 2007: 1700, 2008: 1700,
#     2009: 1750, 2010: 1800, 2011: 2000, 2012: 2200, 2013: 2400, 2014: 2700,
#     2015: 3000, 2016: 3400, 2017: 3800, 2018: 4200, 2019: 4600, 2020: 5000,
#     2021: 5400, 2022: 5800, 2023: 5900, 2024: 6000, 2025: 6100
# }

# def random_date(year):
#     start_date = datetime(year, 1, 1)
#     end_date = datetime(year, 12, 31)
#     delta = end_date - start_date
#     random_days = random.randint(0, delta.days)
#     return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

# def generate_property_record(record_id):
#     location = random.choice(list(locations.keys()))
#     loc_data = locations[location]
#     street, borough = location.rsplit(", ", 1)

#     property_type = random.choices(
#         list(property_types.keys()),
#         weights=[prop["probability"] for prop in property_types.values()],
#         k=1
#     )[0]
#     prop_data = property_types[property_type]

#     size_sqm = round(random.uniform(*prop_data["size_range"]), 1)
#     lot_size_sqm = round(random.uniform(*prop_data["lot_size_range"]), 1)
#     bedrooms = random.randint(*prop_data["bedrooms_range"])
#     bathrooms = max(1, bedrooms // 2) if property_type != "commercial" else 0
#     total_rooms = bedrooms + bathrooms + random.randint(1, 3) if property_type != "commercial" else 0

#     construction_year = random.randint(1950, 2020)
#     if random.random() > 0.5 and construction_year + 5 <= 2024:
#         renovation_year = random.randint(construction_year + 5, 2024)
#         condition = "renovated"
#     else:
#         renovation_year = None
#         condition = random.choice(["new", "renovated", "needs_renovation", "dilapidated"])

#     energy_rating = random.choice(["A", "B", "C", "D", "E"])
#     energy_consumption = round(random.uniform(30, 150), 1)

#     transaction_years = sorted(random.sample(range(2003, 2026), random.randint(1, 3)))
#     transaction_history = []
#     for year in transaction_years:
#         base_price_per_sqm = price_trends[year] * loc_data["price_multiplier"]
#         price_per_sqm = round(base_price_per_sqm * random.uniform(0.9, 1.1), 0)
#         sale_price = round(price_per_sqm * size_sqm, 0)
#         transaction_history.append({
#             "transaction_id": f"TX-{borough[:3].upper()}-{record_id}-{year}",
#             "sale_price_eur": int(sale_price),
#             "transaction_date": random_date(year),
#             "price_per_sqm_eur": int(price_per_sqm),
#             "market_conditions": {
#                 "interest_rate_percent": round(random.uniform(1.0, 5.0), 1),
#                 "demand_index": random.randint(50, 95)
#             }
#         })

#     return {
#         "property_id": f"BER-{borough[:3].upper()}-{record_id}",
#         "address": {
#             "street": street,
#             "postal_code": f"10{random.randint(100, 999)}",
#             "borough": borough
#         },
#         "coordinates": {
#             "latitude": round(loc_data["lat"] + random.uniform(-0.01, 0.01), 4),
#             "longitude": round(loc_data["lon"] + random.uniform(-0.01, 0.01), 4)
#         },
#         "property_type": property_type,
#         "size_sqm": size_sqm,
#         "lot_size_sqm": lot_size_sqm,
#         "rooms": {
#             "bedrooms": bedrooms,
#             "bathrooms": bathrooms,
#             "total_rooms": total_rooms
#         },
#         "construction_year": construction_year,
#         "renovation_year": renovation_year,
#         "condition": condition,
#         "features": {
#             "balcony": random.random() > 0.5,
#             "elevator": random.random() > 0.7 if property_type == "apartment" else False,
#             "heating_type": random.choice(["gas", "district", "electric"]),
#             "parking": random.random() > 0.4 if property_type in ["single_family_house", "multi_family_house"] else False
#         },
#         "energy_rating": energy_rating,
#         "energy_consumption_kwh_m2": energy_consumption,
#         "transaction_history": transaction_history,
#         "proximity": {
#             "public_transport_m": random.randint(50, 500),
#             "schools_m": random.randint(200, 1000),
#             "parks_m": random.randint(100, 800)
#         },
#         "neighborhood_metrics": {
#             "median_income_eur": int(loc_data["median_income"] * random.uniform(0.9, 1.1)),
#             "population_density_per_sqm": int(loc_data["density"] * random.uniform(0.9, 1.1)),
#             "crime_rate_per_1000": round(loc_data["crime_rate"] * random.uniform(0.9, 1.1), 1)
#         },
#         "market_trends": {
#             "avg_price_per_sqm_2003_eur": int(price_trends[2003] * loc_data["price_multiplier"]),
#             "avg_price_per_sqm_2025_eur": int(price_trends[2025] * loc_data["price_multiplier"]),
#             "cap_rate_percent": round(random.uniform(3.0, 5.0), 1),
#             "rental_yield_percent": round(random.uniform(3.0, 5.0), 1)
#         }
#     }

# # Generate records in 5,000 chunks
# all_records = [generate_property_record(i) for i in range(1, 15001)]  # Total 15,000 records for prod
# chunk_size = 5000

# # Dev: 5,000 records, 1 chunk
# dev_records = all_records[:5000]
# dev_key = f"dev/raw/properties/year={current_year}/data_part_1.json"
# s3.put_object(
#     Bucket=bucket_name,
#     Key=dev_key,
#     Body=json.dumps(dev_records, indent=2).encode("utf-8")
# )
# print(f"Uploaded 5,000 records to s3://{bucket_name}/{dev_key}")

# # Prod: 15,000 records, 3 chunks
# for i in range(0, 15000, chunk_size):
#     prod_chunk = all_records[i:i + chunk_size]
#     prod_key = f"prod/raw/properties/year={current_year}/data_part_{i // chunk_size + 1}.json"
#     s3.put_object(
#         Bucket=bucket_name,
#         Key=prod_key,
#         Body=json.dumps(prod_chunk, indent=2).encode("utf-8")
#     )
#     print(f"Uploaded {len(prod_chunk)} records to s3://{bucket_name}/{prod_key}")



import json
import random
import boto3
from datetime import datetime, timedelta
import uuid

# AWS S3 configuration
s3 = boto3.client("s3")
bucket_name = "real-estate-datalake"
current_year = datetime.now().strftime("%Y")

# Real Berlin locations with latitude, longitude, and price multipliers
locations = {
    "Unter den Linden, Mitte": {"lat": 52.5163, "lon": 13.3880, "price_multiplier": 1.25, "median_income": 60000, "density": 4500, "crime_rate": 4.5},
    "Kurfürstendamm, Charlottenburg": {"lat": 52.5030, "lon": 13.3300, "price_multiplier": 1.20, "median_income": 55000, "density": 3800, "crime_rate": 4.0},
    "Oranienstraße, Kreuzberg": {"lat": 52.5000, "lon": 13.4170, "price_multiplier": 1.15, "median_income": 42000, "density": 6200, "crime_rate": 7.0},
    "Kollwitzstraße, Prenzlauer Berg": {"lat": 52.5370, "lon": 13.4160, "price_multiplier": 1.22, "median_income": 53000, "density": 4600, "crime_rate": 4.8},
    "Karl-Marx-Allee, Friedrichshain": {"lat": 52.5150, "lon": 13.4440, "price_multiplier": 1.10, "median_income": 47000, "density": 5000, "crime_rate": 6.0},
    "Hermannstraße, Neukölln": {"lat": 52.4800, "lon": 13.4250, "price_multiplier": 0.95, "median_income": 39000, "density": 5800, "crime_rate": 6.5},
    "Schlossstraße, Steglitz": {"lat": 52.4550, "lon": 13.3150, "price_multiplier": 1.05, "median_income": 48000, "density": 3200, "crime_rate": 4.2},
    "Alt-Kaulsdorf Straße, Marzahn": {"lat": 52.5500, "lon": 13.5800, "price_multiplier": 0.85, "median_income": 35000, "density": 3000, "crime_rate": 5.0},
    "Klosterstraße, Zehlendorf": {"lat": 52.4300, "lon": 13.2500, "price_multiplier": 1.18, "median_income": 57000, "density": 2800, "crime_rate": 3.8},
    "Spandauer Damm, Spandau": {"lat": 52.5400, "lon": 13.2000, "price_multiplier": 0.90, "median_income": 40000, "density": 2900, "crime_rate": 4.5},
    "Potsdamer Straße, Schöneberg": {"lat": 52.4950, "lon": 13.3650, "price_multiplier": 1.15, "median_income": 50000, "density": 4000, "crime_rate": 5.0},
    "Frankfurter Allee, Lichtenberg": {"lat": 52.5150, "lon": 13.4700, "price_multiplier": 0.95, "median_income": 41000, "density": 4500, "crime_rate": 5.5},
    "Rathausstraße, Pankow": {"lat": 52.5600, "lon": 13.4000, "price_multiplier": 1.10, "median_income": 46000, "density": 3400, "crime_rate": 4.3},
    "Tempelhofer Damm, Tempelhof": {"lat": 52.4700, "lon": 13.4000, "price_multiplier": 1.00, "median_income": 45000, "density": 3700, "crime_rate": 4.7},
    "Havelchaussee, Wannsee": {"lat": 52.4200, "lon": 13.1500, "price_multiplier": 1.20, "median_income": 58000, "density": 2500, "crime_rate": 3.5},
    "Biesdorfer Straße, Hellersdorf": {"lat": 52.5300, "lon": 13.6100, "price_multiplier": 0.88, "median_income": 36000, "density": 3100, "crime_rate": 5.2},
    "Reinickendorfer Straße, Wedding": {"lat": 52.5400, "lon": 13.3600, "price_multiplier": 0.98, "median_income": 40000, "density": 4800, "crime_rate": 6.0},
    "Treptower Straße, Treptow": {"lat": 52.4900, "lon": 13.4500, "price_multiplier": 1.05, "median_income": 43000, "density": 4200, "crime_rate": 5.5},
    "Alt-Tegel Straße, Tegel": {"lat": 52.5800, "lon": 13.2700, "price_multiplier": 0.92, "median_income": 41000, "density": 3000, "crime_rate": 4.4},
    "Wuhletalstraße, Köpenick": {"lat": 52.4600, "lon": 13.5800, "price_multiplier": 0.97, "median_income": 42000, "density": 3400, "crime_rate": 4.8}
}

# Property type distribution and characteristics
property_types = {
    "apartment": {"probability": 0.6, "size_range": (30, 120), "lot_size_range": (0, 0), "bedrooms_range": (1, 3)},
    "single_family_house": {"probability": 0.2, "size_range": (100, 250), "lot_size_range": (200, 500), "bedrooms_range": (3, 5)},
    "multi_family_house": {"probability": 0.15, "size_range": (150, 400), "lot_size_range": (300, 600), "bedrooms_range": (4, 8)},
    "commercial": {"probability": 0.05, "size_range": (50, 300), "lot_size_range": (100, 400), "bedrooms_range": (0, 0)}
}

price_trends = {
    2003: 1500, 2004: 1550, 2005: 1600, 2006: 1650, 2007: 1700, 2008: 1700,
    2009: 1750, 2010: 1800, 2011: 2000, 2012: 2200, 2013: 2400, 2014: 2700,
    2015: 3000, 2016: 3400, 2017: 3800, 2018: 4200, 2019: 4600, 2020: 5000,
    2021: 5400, 2022: 5800, 2023: 5900, 2024: 6000, 2025: 6100
}

def random_date(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

def generate_property_record(record_id):
    location = random.choice(list(locations.keys()))
    loc_data = locations[location]
    street, borough = location.rsplit(", ", 1)

    property_type = random.choices(
        list(property_types.keys()),
        weights=[prop["probability"] for prop in property_types.values()],
        k=1
    )[0]
    prop_data = property_types[property_type]

    size_sqm = round(random.uniform(*prop_data["size_range"]), 1)
    lot_size_sqm = round(random.uniform(*prop_data["lot_size_range"]), 1)
    bedrooms = random.randint(*prop_data["bedrooms_range"])
    bathrooms = max(1, bedrooms // 2) if property_type != "commercial" else 0
    total_rooms = bedrooms + bathrooms + random.randint(1, 3) if property_type != "commercial" else 0

    construction_year = random.randint(1950, 2020)
    if random.random() > 0.5 and construction_year + 5 <= 2024:
        renovation_year = random.randint(construction_year + 5, 2024)
        condition = "renovated"
    else:
        renovation_year = None
        condition = random.choice(["new", "renovated", "needs_renovation", "dilapidated"])

    energy_rating = random.choice(["A", "B", "C", "D", "E"])
    energy_consumption = round(random.uniform(30, 150), 1)

    transaction_years = sorted(random.sample(range(2003, 2026), random.randint(1, 3)))
    transaction_history = []
    for year in transaction_years:
        base_price_per_sqm = price_trends[year] * loc_data["price_multiplier"]
        # Introduce negative price_per_sqm for 5% of transactions
        if random.random() < 0.05:
            price_per_sqm = round(random.uniform(-1000, -100), 0)
            sale_price = round(price_per_sqm * size_sqm, 0)
        else:
            price_per_sqm = round(base_price_per_sqm * random.uniform(0.9, 1.1), 0)
            # Introduce negative sale_price for 2% of transactions
            sale_price = round(random.uniform(-50000, -1000), 0) if random.random() < 0.02 else round(price_per_sqm * size_sqm, 0)
        
        transaction_history.append({
            "transaction_id": f"TX-{borough[:3].upper()}-{record_id}-{year}",
            "sale_price_eur": int(sale_price),
            "transaction_date": random_date(year),
            "price_per_sqm_eur": int(price_per_sqm),
            "market_conditions": {
                "interest_rate_percent": round(random.uniform(1.0, 5.0), 1),
                "demand_index": random.randint(50, 95)
            }
        })

    return {
        "property_id": f"BER-{borough[:3].upper()}-{record_id}",
        "address": {
            "street": street,
            "postal_code": f"10{random.randint(100, 999)}",
            "borough": borough
        },
        "coordinates": {
            "latitude": round(loc_data["lat"] + random.uniform(-0.01, 0.01), 4),
            "longitude": round(loc_data["lon"] + random.uniform(-0.01, 0.01), 4)
        },
        "property_type": property_type,
        "size_sqm": size_sqm,
        "lot_size_sqm": lot_size_sqm,
        "rooms": {
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "total_rooms": total_rooms
        },
        "construction_year": construction_year,
        "renovation_year": renovation_year,
        "condition": condition,
        "features": {
            "balcony": random.random() > 0.5,
            "elevator": random.random() > 0.7 if property_type == "apartment" else False,
            "heating_type": random.choice(["gas", "district", "electric"]),
            "parking": random.random() > 0.4 if property_type in ["single_family_house", "multi_family_house"] else False
        },
        "energy_rating": energy_rating,
        "energy_consumption_kwh_m2": energy_consumption,
        "transaction_history": transaction_history,
        "proximity": {
            "public_transport_m": random.randint(50, 500),
            "schools_m": random.randint(200, 1000),
            "parks_m": random.randint(100, 800)
        },
        "neighborhood_metrics": {
            "median_income_eur": int(loc_data["median_income"] * random.uniform(0.9, 1.1)),
            "population_density_per_sqm": int(loc_data["density"] * random.uniform(0.9, 1.1)),
            "crime_rate_per_1000": round(loc_data["crime_rate"] * random.uniform(0.9, 1.1), 1)
        },
        "market_trends": {
            "avg_price_per_sqm_2003_eur": int(price_trends[2003] * loc_data["price_multiplier"]),
            "avg_price_per_sqm_2025_eur": int(price_trends[2025] * loc_data["price_multiplier"]),
            "cap_rate_percent": round(random.uniform(3.0, 5.0), 1),
            "rental_yield_percent": round(random.uniform(3.0, 5.0), 1)
        }
    }

# Empty the prod S3 bucket prefix
def empty_s3_prefix(prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
        print(f"Emptied S3 prefix: s3://{bucket_name}/{prefix}")

# Generate records in 5,000 chunks
all_records = [generate_property_record(i) for i in range(1, 15001)]  # Total 15,000 records for prod
chunk_size = 5000

# Clear prod prefix before uploading
prod_prefix = f"prod/raw/properties/year={current_year}/"
empty_s3_prefix(prod_prefix)

# Prod: 15,000 records, 3 chunks
for i in range(0, 15000, chunk_size):
    prod_chunk = all_records[i:i + chunk_size]
    prod_key = f"prod/raw/properties/year={current_year}/data_part_{i // chunk_size + 1}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=prod_key,
        Body=json.dumps(prod_chunk, indent=2).encode("utf-8")
    )
    print(f"Uploaded {len(prod_chunk)} records to s3://{bucket_name}/{prod_key}")