import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns


print("📂 Loading dataset...")
df = dd.read_csv("airlines.csv")

print("✅ Columns found:", list(df.columns))


df = df.dropna(subset=["Age", "Gender", "Country Name", "Flight Status"])
df["Age"] = df["Age"].astype(float)

# Convert Dask dataframe to Pandas for plotting
print("🧠 Collecting a small sample for visualization...")
sample_df = df.sample(frac=0.2, random_state=42).compute()

plt.figure(figsize=(6, 4))
sns.countplot(data=sample_df, x="Gender", palette="Set2")
plt.title("Gender Distribution of Passengers")
plt.xlabel("Gender")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("output/gender_distribution.png")
plt.show()

plt.figure(figsize=(6, 4))
sns.countplot(data=sample_df, x="Flight Status", palette="coolwarm")
plt.title("Flight Status Overview")
plt.xlabel("Flight Status")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("output/flight_status.png")
plt.show()

plt.figure(figsize=(7, 5))
sns.barplot(data=sample_df, x="Airport Continent", y="Age", estimator="mean", palette="muted")
plt.title("Average Passenger Age by Continent")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/age_by_continent.png")
plt.show()

top_countries = (
    sample_df["Country Name"]
    .value_counts()
    .head(10)
    .reset_index()
)

top_countries.columns = ["Country", "Count"]

plt.figure(figsize=(8, 5))
sns.barplot(data=top_countries, x="Country", y="Count", palette="crest")
plt.title("Top 10 Passenger Nationalities")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/top_countries.png")
plt.show()

print("✅ All visualizations saved in the 'output' folder!")
