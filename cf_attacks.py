#!/usr/bin/env python3
"""
cf_attacks.py — Fetch hourly Layer 7 attack traffic percentage from
Cloudflare Radar, persist to DynamoDB, render a sparkline, and sync to S3.

Each hour this produces a single data point: what percentage of total
HTTP traffic was attack traffic.  The chart is a clean rolling time series.

Environment variables:
    CF_API_TOKEN          Cloudflare Radar API token  (required — mount via K8s Secret)
    DYNAMODB_TABLE        DynamoDB table name          (default: cf-attack-timeseries)
    S3_BUCKET             S3 bucket for chart output   (required)
    S3_PREFIX             Key prefix inside the bucket (default: charts/)
    AWS_REGION            AWS region                   (default: us-east-1)
    DATE_RANGE            Radar date range             (default: 2d)
    AGG_INTERVAL          Aggregation interval         (default: 1h)
    CHART_HOURS           How many hours of history to show from DynamoDB (default: 168 = 7 days)

License note: Cloudflare Radar data is CC BY-NC 4.0.
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from io import BytesIO

import boto3
import pandas as pd
import requests
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from botocore.config import Config as BotoConfig

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CF_API_TOKEN   = os.environ.get("CF_API_TOKEN", "")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "cf-attack-timeseries")
S3_BUCKET      = os.environ.get("S3_BUCKET", "")
S3_PREFIX      = os.environ.get("S3_PREFIX", "charts/")
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")
DATE_RANGE     = os.environ.get("DATE_RANGE", "2d")
AGG_INTERVAL   = os.environ.get("AGG_INTERVAL", "1h")
CHART_HOURS    = int(os.environ.get("CHART_HOURS", "168"))  # 7 days

API_BASE = "https://api.cloudflare.com/client/v4/radar/attacks/layer7"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
_boto_cfg = BotoConfig(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "adaptive"},
)

def _dynamodb_table():
    return boto3.resource("dynamodb", config=_boto_cfg).Table(DYNAMODB_TABLE)

def _s3_client():
    return boto3.client("s3", config=_boto_cfg)


# ---------------------------------------------------------------------------
# Cloudflare API
# ---------------------------------------------------------------------------
def _headers() -> dict:
    return {"Authorization": f"Bearer {CF_API_TOKEN}"}

def _get(url: str, params: dict) -> dict:
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"Cloudflare API error: {body.get('errors')}")
    return body


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def fetch_attack_percentage() -> pd.DataFrame:
    """
    Fetch the overall L7 attack traffic percentage from Radar.
    Returns a DataFrame with columns: [timestamp, pct_attacks]
    """
    log.info("Fetching L7 attack percentage (range=%s, interval=%s) ...",
             DATE_RANGE, AGG_INTERVAL)

    body = _get(f"{API_BASE}/timeseries", {
        "dateRange": DATE_RANGE,
        "aggInterval": AGG_INTERVAL,
        "format": "json",
    })
    serie = body["result"]["serie_0"]

    df = pd.DataFrame({
        "timestamp":   pd.to_datetime(serie["timestamps"], utc=True),
        "pct_attacks": pd.to_numeric(serie["values"]),
    })

    log.info("  -> %d hourly data points (%.2f%% mean, %.2f%% peak).",
             len(df), df["pct_attacks"].mean(), df["pct_attacks"].max())
    return df


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------
def save_to_dynamodb(df: pd.DataFrame) -> int:
    """
    Upsert hourly data points.

    Table schema:
        PK:  metric     (String)  — always "l7_attack_pct" for now
        SK:  timestamp  (String)  — ISO-8601
    """
    table = _dynamodb_table()
    fetched_at = datetime.now(timezone.utc).isoformat()
    written = 0

    with table.batch_writer(overwrite_by_pkeys=["metric", "timestamp"]) as batch:
        for row in df.itertuples(index=False):
            batch.put_item(Item={
                "metric":     "l7_attack_pct",
                "timestamp":  row.timestamp.isoformat(),
                "value":      Decimal(str(round(row.pct_attacks, 4))),
                "fetched_at": fetched_at,
            })
            written += 1

    log.info("Wrote %d items to DynamoDB table '%s'.", written, DYNAMODB_TABLE)
    return written


def load_history_from_dynamodb(hours: int = CHART_HOURS) -> pd.DataFrame:
    """
    Query DynamoDB for the last N hours of data points.
    Returns a DataFrame sorted by timestamp.
    """
    table = _dynamodb_table()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    resp = table.query(
        KeyConditionExpression="metric = :m AND #ts >= :cutoff",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ExpressionAttributeValues={
            ":m":      "l7_attack_pct",
            ":cutoff": cutoff,
        },
    )

    items = resp.get("Items", [])
    while resp.get("LastEvaluatedKey"):
        resp = table.query(
            KeyConditionExpression="metric = :m AND #ts >= :cutoff",
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={
                ":m":      "l7_attack_pct",
                ":cutoff": cutoff,
            },
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    if not items:
        log.warning("No historical data in DynamoDB for the last %d hours.", hours)
        return pd.DataFrame(columns=["timestamp", "pct_attacks"])

    df = pd.DataFrame(items)
    df["timestamp"]   = pd.to_datetime(df["timestamp"], utc=True)
    df["pct_attacks"] = df["value"].astype(float)
    df = df[["timestamp", "pct_attacks"]].sort_values("timestamp").reset_index(drop=True)
    df = df.drop_duplicates(subset="timestamp", keep="last")

    log.info("Loaded %d historical points from DynamoDB (last %d hours).",
             len(df), hours)
    return df


# ---------------------------------------------------------------------------
# Chart — clean sparkline
# ---------------------------------------------------------------------------
def generate_chart(df: pd.DataFrame) -> BytesIO:
    """
    Render a single time series of attack traffic percentage.
    """
    sns.set_theme(style="whitegrid", font_scale=0.9)

    fig, ax = plt.subplots(figsize=(12, 3.5))

    # Main line
    sns.lineplot(
        data=df, x="timestamp", y="pct_attacks",
        color="#d62828", linewidth=1.3, ax=ax,
    )

    # Subtle fill
    ax.fill_between(df["timestamp"], df["pct_attacks"],
                    alpha=0.12, color="#d62828")

    # Rolling average if enough points
    if len(df) >= 6:
        df = df.copy()
        df["rolling_avg"] = df["pct_attacks"].rolling(window=6, center=True).mean()
        ax.plot(df["timestamp"], df["rolling_avg"],
                color="#264653", linewidth=1.8, alpha=0.7,
                label="6h rolling avg")
        ax.legend(loc="upper right", fontsize=8, framealpha=0.8)

    # Axis labels
    ax.set_ylabel("Attack traffic %", fontsize=10)
    ax.set_xlabel("")
    ax.set_title("Layer 7 Attack Traffic — Hourly", fontsize=11,
                 fontweight="bold", pad=10)
    ax.grid(axis="y", alpha=0.25)
    ax.grid(axis="x", alpha=0.1)

    # Smart x-axis ticks
    hours_span = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
    if hours_span <= 48:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
    elif hours_span <= 168:
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))

    fig.autofmt_xdate(rotation=30)

    ## commenting out the annotation on last value for now ... hard to read in plot
    # # Annotate latest value
    # if not df.empty:
    #     latest = df.iloc[-1]
    #     ax.annotate(
    #         f'{latest["pct_attacks"]:.2f}%',
    #         xy=(latest["timestamp"], latest["pct_attacks"]),
    #         xytext=(10, 10), textcoords="offset points",
    #         fontsize=9, fontweight="bold", color="#d62828",
    #         arrowprops=dict(arrowstyle="->", color="#d62828", lw=0.8),
    #     )

    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    log.info("Chart rendered (%d KB).", buf.getbuffer().nbytes // 1024)
    return buf


# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------
def upload_chart_to_s3(buf: BytesIO) -> str:
    now_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{S3_PREFIX}attacks_{now_tag}.png"

    _s3_client().put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=buf.getvalue(), ContentType="image/png",
    )
    _s3_client().put_object(
        Bucket=S3_BUCKET, Key=f"{S3_PREFIX}attacks_latest.png",
        Body=buf.getvalue(), ContentType="image/png",
    )

    uri = f"s3://{S3_BUCKET}/{key}"
    log.info("Chart uploaded -> %s  (+ latest copy)", uri)
    return uri


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    errors = []
    if not CF_API_TOKEN:
        errors.append("CF_API_TOKEN is not set.")
    if not S3_BUCKET:
        errors.append("S3_BUCKET is not set.")
    if errors:
        for e in errors:
            log.error(e)
        sys.exit(1)

    # 1. Fetch latest window from Cloudflare
    fresh_df = fetch_attack_percentage()

    # 2. Persist to DynamoDB
    save_to_dynamodb(fresh_df)

    # 3. Pull full history from DynamoDB for the chart
    #    This gives us a longer view than a single API call.
    history_df = load_history_from_dynamodb(hours=CHART_HOURS)

    # Fall back to just the fresh data if DynamoDB is empty (first run)
    chart_df = history_df if not history_df.empty else fresh_df

    log.info("Charting %d data points spanning %s -> %s",
             len(chart_df),
             chart_df["timestamp"].min().strftime("%Y-%m-%d %H:%M"),
             chart_df["timestamp"].max().strftime("%Y-%m-%d %H:%M"))

    # 4. Render and upload
    chart_buf = generate_chart(chart_df)
    upload_chart_to_s3(chart_buf)

    log.info("Done.")


if __name__ == "__main__":
    main()
