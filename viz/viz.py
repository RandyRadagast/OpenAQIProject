import pandas as pd
import plotly.express as px
import logging
from pathlib import Path
from datetime import datetime, timedelta, UTC

#detect proj root to resolve 'I cant find this' errors.
scriptDir = Path(__file__).resolve().parent
projectRoot = scriptDir.parent

#Define block
vizDir = scriptDir
vizDir.mkdir(parents=True, exist_ok=True)
logFile = vizDir / "viz.log"
cleanDir = projectRoot / "data" / "clean"
chartOutput = vizDir / "chart.html"

#Setting up logging!
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(logFile),
        logging.StreamHandler()
    ]
)

#locate only 3 weeks of files, which should also be 3 weeks of data if run daily
cutoffDate = datetime.now(UTC) - timedelta(days=21)
csvFiles = sorted(cleanDir.glob("cleaned_data_*.csv"))

recentFiles = []
for file in csvFiles:
    try:
        dateStr = file.stem.replace("cleaned_data_", "")
        fileDate = datetime.strptime(dateStr, "%Y-%m-%d").replace(tzinfo=UTC)
        if fileDate >= cutoffDate:
            recentFiles.append(file)
    except ValueError:
        continue

if not recentFiles:
    logging.error("No cleaned data files found within the last 21 days.")
    raise SystemExit(1)

logging.info(f"Found {len(recentFiles)} recent cleaned files.")
for f in recentFiles:
    logging.info(f"  - {f.name}")

#Read to df
frames = []
for file in recentFiles:
    df = pd.read_csv(file)
    df["source_file"] = file.name
    frames.append(df)

df = pd.concat(frames, ignore_index=True)
logging.info(f"Combined {len(df)} total rows from recent files.")

# yet another filtering step
requiredCols = {"sensor_name", "sensor_tag", "avg_value", "datetime_from"}
missing = requiredCols - set(df.columns)
if missing:
    logging.error(f"Missing required columns: {missing}")
    raise SystemExit(1)

df["datetime_from"] = pd.to_datetime(df["datetime_from"], errors="coerce", utc=True)
df = df.dropna(subset=["datetime_from", "avg_value", "sensor_name"])
df = df[df["sensor_tag"].isin(["pm25", "o3", "rh"])]

if df.empty:
    logging.error("No valid data after filtering (pm25, o3, rh).")
    raise SystemExit(1)

# aggregate sensor data
df["date"] = df["datetime_from"].dt.date
daily = (
    df.groupby(["sensor_name", "sensor_tag", "date"], as_index=False)
      .agg(avg_value=("avg_value", "mean"))
)

logging.info(f"Prepared {len(daily)} daily rows for plotting")

# Plotly chart
fig = px.line(
    daily,
    x="date",
    y="avg_value",
    color="sensor_name",
    hover_data={"sensor_name": True, "sensor_tag": True, "avg_value": ":.4f"},
    title="Daily Air Quality per Sensor (O₃, PM₂.₅, RH) — Last 3 Weeks — Maine"
)

fig.update_layout(
    xaxis_title="Date (UTC)",
    yaxis_title="Average Value",
    legend_title="Sensor",
    template="plotly_white"
)

#save chart to html file.
fig.write_html(chartOutput, include_plotlyjs="cdn")
logging.info(f"Chart saved to {chartOutput}")

print(f"Visualization complete: {chartOutput}")
