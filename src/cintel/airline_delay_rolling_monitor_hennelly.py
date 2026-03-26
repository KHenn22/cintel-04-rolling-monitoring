"""
airline_delay_rolling_monitor_hennelly.py - Project script.

Author: Denise Case, Kevin Hennelly
Date: 2026-03

Time-Series System Metrics Data

- Data is taken from a system that records operational metrics over time.
- Each row represents one observation at a specific timestamp.
- The CSV file includes these columns:
  - timestamp: when the observation occurred
  - requests: number of requests handled
  - errors: number of failed requests
  - total_latency_ms: total response time in milliseconds

Purpose

- Read time-series system metrics from a CSV file.
- Demonstrate rolling monitoring using a moving window.
- Compute rolling averages to smooth short-term variation.
- Save the resulting monitoring signals as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- How does system behavior change over time?
- Why might a rolling average reveal patterns that individual observations hide?
- How can smoothing short-term variation help us understand longer-term trends?

Paths (relative to repo root)

    INPUT FILE: data/T_ONTIME_REPORTING_OCT_2025.csv
    OUTPUT FILE: artifacts/airline_delay_rolling_metrics_hennelly.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.airline_delay_rolling_monitor_hennelly

OBS:
  Don't edit this file - it should remain a working example.
  Use as much of this code as you can when creating your own pipeline script,
  and change the monitoring logic as needed for your project.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "T_ONTIME_REPORTING_OCT_2025.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "airline_delay_rolling_metrics_hennelly.csv"

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_FILE", DATA_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure artifacts directory exists
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ CSV DATA FILE INTO A POLARS DATAFRAME (TABLE)
    # ----------------------------------------------------
    df = pl.read_csv(DATA_FILE, infer_schema_length=10000, ignore_errors=True)

    LOG.info(f"Loaded {df.height} flight records")

    # --------------------------------------------------
    # STEP 2: AGGREGATE BY DATE (FL_DATE)
    # Each row in the aggregated df = one day of flights
    # - flights    : total number of flights that day
    # - cancellations: number of cancelled flights
    # - avg_dep_delay: mean departure delay (minutes)
    # --------------------------------------------------
    df_daily = (
        df.with_columns(
            pl.col("FL_DATE")
            .str.strptime(pl.Date, "%m/%d/%Y %I:%M:%S %p", strict=False)
            .alias("timestamp")
        )
        .group_by(["timestamp", "OP_UNIQUE_CARRIER"])
        .agg(
            [
                pl.len().alias("flights"),
                pl.col("CANCELLED").sum().alias("cancellations"),
                pl.col("DEP_DELAY_NEW").mean().alias("avg_dep_delay"),
            ]
        )
        .sort(["OP_UNIQUE_CARRIER", "timestamp"])
    )

    LOG.info(f"Aggregated into {df_daily.height} daily records")

    # ----------------------------------------------------
    # STEP 3: DEFINE ROLLING WINDOW RECIPES
    # ----------------------------------------------------
    # A rolling window computes statistics over the most recent
    # N observations. The window "moves" forward one row at a time.

    # Example: if WINDOW_SIZE = 3
    # row 1 → mean of rows [1]
    # row 2 → mean of rows [1,2]
    # row 3 → mean of rows [1,2,3]
    # row 4 → mean of rows [2,3,4]

    WINDOW_SIZE: int = 5

    # ----------------------------------------------------
    # STEP 3.1: DEFINE ROLLING MEAN FOR # OF FLIGHTS
    # ----------------------------------------------------
    # The `flights` column holds the count of flights at each timestamp.
    flights_rolling_mean_recipe: pl.Expr = (
        pl.col("flights")
        .rolling_mean(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("flights_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.2: DEFINE ROLLING MEAN FOR # OF CANCELLATIONS
    # ----------------------------------------------------
    # The `cancellations` column holds the count of cancelled flights at each timestamp.
    cancellations_rolling_mean_recipe: pl.Expr = (
        pl.col("cancellations")
        .rolling_mean(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("cancellations_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.3: DEFINE ROLLING MEAN FOR AVG DEPARTURE DELAY
    # ----------------------------------------------------
    # The `avg_dep_delay` column holds the average departure delay in minutes at each timestamp.
    avg_dep_delay_rolling_mean_recipe: pl.Expr = (
        pl.col("avg_dep_delay")
        .rolling_mean(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("delay_rolling_mean")
    )

    # STEP 3.3.1: DEFINE ROLLING STD FOR # OF FLIGHTS
    flights_rolling_std_recipe: pl.Expr = (
        pl.col("flights")
        .rolling_std(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("flights_rolling_std")
    )

    # STEP 3.3.2: DEFINE ROLLING STD FOR # OF CANCELLATIONS
    cancellations_rolling_std_recipe: pl.Expr = (
        pl.col("cancellations")
        .rolling_std(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("cancellations_rolling_std")
    )

    # STEP 3.3.3: DEFINE ROLLING STD FOR AVG DEPARTURE DELAY
    avg_dep_delay_rolling_std_recipe: pl.Expr = (
        pl.col("avg_dep_delay")
        .rolling_std(WINDOW_SIZE)
        .over("OP_UNIQUE_CARRIER")
        .alias("delay_rolling_std")
    )
    # ----------------------------------------------------
    # STEP 3.4: APPLY THE ROLLING RECIPES IN A NEW DATAFRAME
    # ----------------------------------------------------
    # with_columns() evaluates the recipes and adds the new columns
    df_with_rolling = df_daily.with_columns(
        [
            flights_rolling_mean_recipe,
            cancellations_rolling_mean_recipe,
            avg_dep_delay_rolling_mean_recipe,
            flights_rolling_std_recipe,
            cancellations_rolling_std_recipe,
            avg_dep_delay_rolling_std_recipe,
        ]
    )

    LOG.info("Computed rolling mean signals")

    # STEP 3.4.1: FLAG ANOMALIES (value exceeds mean + 1 std deviations)
    df_with_rolling = df_with_rolling.with_columns(
        [
            (
                pl.col("cancellations")
                > pl.col("cancellations_rolling_mean")
                + 1 * pl.col("cancellations_rolling_std")
            ).alias("cancellation_spike_flag"),
            (
                pl.col("avg_dep_delay")
                > pl.col("delay_rolling_mean") + 1 * pl.col("delay_rolling_std")
            ).alias("delay_spike_flag"),
        ]
    )
    # STEP 3.5: SUMMARIZE SPIKES BY AIRLINE
    summary = (
        df_with_rolling.group_by("OP_UNIQUE_CARRIER")
        .agg(
            [
                pl.col("delay_spike_flag").sum().alias("delay_spikes"),
                pl.col("cancellation_spike_flag").sum().alias("cancellation_spikes"),
                pl.len().alias("total_days"),
            ]
        )
        .sort("delay_spikes", descending=True)
    )

    pl.Config.set_tbl_rows(25)
    LOG.info("\n" + str(summary))
    # ----------------------------------------------------
    # STEP 4: SAVE RESULTS AS AN ARTIFACT
    # ----------------------------------------------------
    df_with_rolling.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote rolling monitoring file: {OUTPUT_FILE}")
    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
