import dask.dataframe as dd
import pandas as pd

print("🚀 Loading dataset with Dask...")
df = dd.read_csv("airlines.csv")

print("\n✅ Columns detected:")
print(df.columns)

# --- CLEANING ---
print("\n🧹 Cleaning data...")
df = df.dropna(subset=["Gender", "Age", "Country Name", "Flight Status"])
df["Age"] = df["Age"].astype(float)
df["Departure Date"] = dd.to_datetime(df["Departure Date"], errors="coerce")

# --- BASIC ANALYTICS ---
print("\n📊 Computing basic insights...")
avg_age = df["Age"].mean().compute()
total_passengers = len(df)
status_counts = df["Flight Status"].value_counts().compute()
continent_counts = df["Airport Continent"].value_counts().compute()

print(f"\nTotal Passengers: {total_passengers}")
print(f"Average Age: {avg_age:.2f}\n")

print("🛫 Flight Status Distribution:")
print(status_counts)

print("\n🌍 Flights by Continent:")
print(continent_counts)

# --- ADVANCED INSIGHT ---
print("\n🔍 Finding Top 5 Countries by Passenger Count...")
top_countries = (
    df.groupby("Country Name")["Passenger ID"]
    .count()
    .nlargest(5)
    .compute()
)
print(top_countries)

# --- EXPORT CLEANED DATA ---
print("\n💾 Writing cleaned dataset to 'output/cleaned_airlines.csv'...")
df.compute().to_csv("output/cleaned_airlines.csv", index=False)
print("✅ Done! Data cleaned and saved successfully.")
