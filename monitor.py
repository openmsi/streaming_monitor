#!/usr/bin/env python3
"""
Streaming pipeline monitor.

Reconciles producer logs, consumer logs, and Girder folder contents.

Usage:
    python monitor.py \\
        --producer-logs ./producer_logs \\
        --consumer-logs ./consumer_logs \\
        --girder-api-url https://girder.htmdec.org/api/v1 \\
        --girder-api-key <key> \\
        --topic-name aimdl-lasershock-otherdata \\
        [--report report.txt] \\
        [--png pipeline_status.png]
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
from girder_client import GirderClient


TIMESTAMP_FMT = "%a %b %d, %Y at %H:%M:%S"

# Color families per stage
COLORS = {
    "produced": {
        "completed": "#1565C0",  # dark blue
        "in_progress": "#64B5F6",  # medium blue
        "unknown": "#E3F2FD",  # pale blue
    },
    "consumed": {
        "completed": "#2E7D32",  # dark green
        "in_progress": "#81C784",  # medium green
        "failed": "#EF9A9A",  # light red — makes failures pop
        "unknown": "#F1F8E9",  # pale green (not yet reached)
    },
    "girder": {
        "present": "#00695C",  # dark teal
        "absent": "#E0F2F1",  # pale teal
    },
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _strip(s):
    return str(s).strip("'").strip()


def _parse_ts(s):
    try:
        return datetime.strptime(_strip(s), TIMESTAMP_FMT)
    except Exception:
        return None


def load_producer_logs(producer_dir):
    rows = []

    for path in Path(producer_dir).glob("uploaded_to_*.csv"):
        df = pd.read_csv(path, sep=";")
        df.columns = df.columns.str.strip()
        for _, row in df.iterrows():
            rows.append(
                {
                    "filename": _strip(row["filename"]),
                    "rel_filepath": _strip(row["rel_filepath"]),
                    "produced_start": _parse_ts(row["started"]),
                    "produced_end": _parse_ts(row["completed"]),
                    "produce_status": "completed",
                }
            )

    for path in Path(producer_dir).glob("upload_to_*_in_progress.csv"):
        df = pd.read_csv(path, sep=";")
        df.columns = df.columns.str.strip()
        for _, row in df.iterrows():
            rows.append(
                {
                    "filename": _strip(row["filename"]),
                    "rel_filepath": _strip(row["rel_filepath"]),
                    "produced_start": _parse_ts(row["started"]),
                    "produced_end": None,
                    "produce_status": "in_progress",
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "filename",
                "rel_filepath",
                "produced_start",
                "produced_end",
                "produce_status",
            ]
        )

    df = pd.DataFrame(rows).drop_duplicates("filename")
    return df


def load_consumer_logs(consumer_dir):
    rows = []

    for path in Path(consumer_dir).glob("processed_from_*.csv"):
        df = pd.read_csv(path, sep=";")
        df.columns = df.columns.str.strip()
        for _, row in df.iterrows():
            rows.append(
                {
                    "filename": _strip(row["filename"]),
                    "consumed_start": _parse_ts(row["first_message"]),
                    "consumed_end": _parse_ts(row["processed_at"]),
                    "consume_status": "completed",
                }
            )

    for path in Path(consumer_dir).glob("consuming_from_*_in_progress_*.csv"):
        df = pd.read_csv(path, sep=";")
        df.columns = df.columns.str.strip()
        for _, row in df.iterrows():
            status = _strip(row.get("status", "in_progress"))
            rows.append(
                {
                    "filename": _strip(row["filename"]),
                    "consumed_start": _parse_ts(row["first_message"]),
                    "consumed_end": None,
                    "consume_status": status,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=["filename", "consumed_start", "consumed_end", "consume_status"]
        )

    df = pd.DataFrame(rows)
    # If a file appears in both completed and in_progress/failed, prefer completed
    completed = set(df.loc[df["consume_status"] == "completed", "filename"])
    df = df[~((df["consume_status"] != "completed") & (df["filename"].isin(completed)))]
    return df.drop_duplicates("filename")


def get_girder_filenames(api_url, api_key, topic_name):
    import json

    client = GirderClient(apiUrl=api_url)
    client.authenticate(apiKey=api_key)
    items = client.get(
        "item/query",
        parameters={
            "query": json.dumps({"meta.KafkaTopic": topic_name}),
            "limit": 100000,
        },
    )
    return {item["name"] for item in items}


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _fmt_duration(seconds):
    if seconds is None or (isinstance(seconds, float) and pd.isna(seconds)):
        return "—"
    s = int(abs(seconds))
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        return f"{s // 60}m {s % 60}s"
    else:
        return f"{s // 3600}h {(s % 3600) // 60}m"


def _timing_stats(series):
    s = series.dropna()
    if s.empty:
        return "—", "—", "—"
    return _fmt_duration(s.min()), _fmt_duration(s.median()), _fmt_duration(s.max())


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def generate_report(merged, output_path):
    total = len(merged)
    produced = (merged["produce_status"] == "completed").sum()
    prod_in_prog = (merged["produce_status"] == "in_progress").sum()
    consumed = (merged["consume_status"] == "completed").sum()
    cons_failed = (merged["consume_status"] == "failed").sum()
    cons_in_prog = (merged["consume_status"] == "in_progress").sum()
    not_consumed = merged["consume_status"].isna().sum()
    on_girder = merged["on_girder"].sum()

    merged = merged.copy()
    merged["produce_duration_s"] = (
        merged["produced_end"] - merged["produced_start"]
    ).dt.total_seconds()
    merged["broker_lag_s"] = (
        merged["consumed_start"] - merged["produced_end"]
    ).dt.total_seconds()
    merged["consume_duration_s"] = (
        merged["consumed_end"] - merged["consumed_start"]
    ).dt.total_seconds()
    merged["total_duration_s"] = (
        merged["consumed_end"] - merged["produced_start"]
    ).dt.total_seconds()

    lines = []
    L = lines.append
    SEP = "=" * 70
    DIV = "-" * 70

    L(SEP)
    L("STREAMING PIPELINE REPORT")
    L(SEP)
    L(f"Total files tracked:              {total}")
    L(f"  Produced  — completed:          {produced}")
    L(f"  Produced  — in progress:        {prod_in_prog}")
    L(f"  Consumed  — completed:          {consumed}")
    L(f"  Consumed  — failed:             {cons_failed}")
    L(f"  Consumed  — in progress:        {cons_in_prog}")
    L(f"  Consumed  — not reached:        {not_consumed}")
    L(f"  On Girder:                      {on_girder}")
    L("")
    L(DIV)
    L("TIMING  (min / median / max)")
    L(DIV)
    for col, label in [
        ("produce_duration_s", "Produce duration        "),
        ("broker_lag_s", "Broker lag (produced→consumed)"),
        ("consume_duration_s", "Consume duration        "),
        ("total_duration_s", "Total (produce→consumed)"),
    ]:
        mn, med, mx = _timing_stats(merged[col])
        L(f"  {label}  {mn} / {med} / {mx}")
    L("")

    problems = {
        "PRODUCED BUT NOT CONSUMED": merged[
            merged["consume_status"].isna() & (merged["produce_status"] == "completed")
        ],
        "FAILED CONSUMPTION": merged[merged["consume_status"] == "failed"],
        "CONSUMED BUT NOT ON GIRDER": merged[
            (merged["consume_status"] == "completed") & (~merged["on_girder"])
        ],
    }
    any_problems = False
    for heading, df in problems.items():
        if not df.empty:
            any_problems = True
            df = df.sort_values("produced_start", ascending=False, na_position="last")
            L(DIV)
            L(f"{heading}  ({len(df)})  — most recent first")
            L(DIV)
            for _, row in df.iterrows():
                ts = row["produced_start"]
                ts_str = ts.strftime("%Y-%m-%d %H:%M") if pd.notna(ts) else "unknown time"
                L(f"  [{ts_str}]  {row['filename']}")
            L("")

    if not any_problems:
        L("No issues detected.")
        L("")

    L(SEP)

    report = "\n".join(lines)
    print(report)
    if output_path:
        Path(output_path).write_text(report)
        print(f"\nReport saved to {output_path}")

    return merged


# ---------------------------------------------------------------------------
# PNG visualization
# ---------------------------------------------------------------------------


def generate_png(merged, output_path):
    total = len(merged)
    # Only show files with at least one issue, most recent first
    df = merged[
        (~merged["on_girder"])
        | (merged["consume_status"] == "failed")
        | (merged["produce_status"] != "completed")
    ].sort_values("produced_start", ascending=False, na_position="last").reset_index(drop=True)
    n = len(df)

    if n == 0:
        print(f"PNG skipped — all {total} files are fully on Girder with no issues.")
        return

    def prod_color(row):
        s = row.get("produce_status", None)
        return COLORS["produced"].get(
            s if s in COLORS["produced"] else "unknown", COLORS["produced"]["unknown"]
        )

    def cons_color(row):
        s = row.get("consume_status", None)
        if pd.isna(s) if not isinstance(s, str) else s is None:
            return COLORS["consumed"]["unknown"]
        return COLORS["consumed"].get(
            s if s in COLORS["consumed"] else "unknown", COLORS["consumed"]["unknown"]
        )

    def girder_color(row):
        return (
            COLORS["girder"]["present"]
            if row.get("on_girder")
            else COLORS["girder"]["absent"]
        )

    col_labels = ["Produced", "Consumed", "On Girder"]
    color_fns = [prod_color, cons_color, girder_color]
    n_cols = len(col_labels)
    cell_w = 1.0
    cell_h = 0.6
    label_w = 6.0   # width reserved for file labels on the left
    date_w  = 1.4   # width for produced-at date
    timing_w = 1.2  # width for total time

    fig_w = label_w + date_w + n_cols * cell_w + timing_w + 0.4
    fig_h = max(4.0, n * cell_h + 2.0)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.set_xlim(-label_w, date_w + n_cols * cell_w + timing_w)
    ax.set_ylim(0, n * cell_h)
    ax.axis("off")

    # Column headers
    ax.text(
        date_w / 2, n * cell_h + 0.15, "Produced at",
        ha="center", va="bottom", fontsize=7, fontweight="bold", color="#555555",
    )
    for col_idx, label in enumerate(col_labels):
        ax.text(
            date_w + col_idx * cell_w + cell_w / 2,
            n * cell_h + 0.15,
            label,
            ha="center", va="bottom", fontsize=8, fontweight="bold",
        )
    ax.text(
        date_w + n_cols * cell_w + timing_w / 2,
        n * cell_h + 0.15,
        "Total time",
        ha="center", va="bottom", fontsize=8, fontweight="bold",
    )

    for row_idx, (_, row) in enumerate(df.iterrows()):
        y = (n - row_idx - 1) * cell_h

        # File label
        fname = row["filename"]
        label = fname if len(fname) <= 55 else "…" + fname[-52:]
        ax.text(-0.1, y + cell_h / 2, label, ha="right", va="center", fontsize=5)

        # Produced-at date
        ts = row.get("produced_start")
        ts_str = ts.strftime("%Y-%m-%d %H:%M") if pd.notna(ts) else "—"
        ax.text(
            date_w / 2, y + cell_h / 2, ts_str,
            ha="center", va="center", fontsize=5, color="#555555",
        )

        # Stage cells
        for col_idx, (label, color_fn) in enumerate(zip(col_labels, color_fns)):
            color = color_fn(row)
            rect = plt.Rectangle(
                (date_w + col_idx * cell_w, y), cell_w, cell_h,
                color=color, ec="white", lw=0.5,
            )
            ax.add_patch(rect)

        # Total time label
        total_s = row.get("total_duration_s")
        total_label = _fmt_duration(total_s)
        ax.text(
            date_w + n_cols * cell_w + timing_w / 2,
            y + cell_h / 2,
            total_label,
            ha="center", va="center", fontsize=6, color="#333333",
        )

    # Legend
    legend_elements = [
        # Produced
        mpatches.Patch(
            color=COLORS["produced"]["completed"], label="Produced — completed"
        ),
        mpatches.Patch(
            color=COLORS["produced"]["in_progress"], label="Produced — in progress"
        ),
        mpatches.Patch(color=COLORS["produced"]["unknown"], label="Produced — unknown"),
        # Consumed
        mpatches.Patch(
            color=COLORS["consumed"]["completed"], label="Consumed — completed"
        ),
        mpatches.Patch(
            color=COLORS["consumed"]["in_progress"], label="Consumed — in progress"
        ),
        mpatches.Patch(color=COLORS["consumed"]["failed"], label="Consumed — failed"),
        mpatches.Patch(
            color=COLORS["consumed"]["unknown"], label="Consumed — not reached"
        ),
        # Girder
        mpatches.Patch(color=COLORS["girder"]["present"], label="On Girder"),
        mpatches.Patch(color=COLORS["girder"]["absent"], label="Not on Girder"),
    ]
    ax.legend(
        handles=legend_elements,
        loc="lower right",
        bbox_to_anchor=(1.0, -0.12),
        ncol=3,
        fontsize=6,
        frameon=True,
        framealpha=0.9,
    )

    ok_count = total - n
    plt.title(
        f"Streaming Pipeline — Files with Issues  ({n} shown, {ok_count}/{total} fully on Girder)",
        fontsize=9,
        pad=10,
    )
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"PNG saved to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Streaming pipeline monitor")
    parser.add_argument(
        "--producer-logs", required=True, help="Dir with producer log CSVs"
    )
    parser.add_argument(
        "--consumer-logs", required=True, help="Dir with consumer log CSVs"
    )
    parser.add_argument("--girder-api-url", default=None, help="Girder API base URL")
    parser.add_argument("--girder-api-key", default=None, help="Girder API key")
    parser.add_argument(
        "--topic-name", default=None, help="Kafka topic name (filters Girder items by meta.KafkaTopic)"
    )
    parser.add_argument(
        "--report", default="report.txt", help="Output path for text report"
    )
    parser.add_argument(
        "--png", default="pipeline_status.png", help="Output path for PNG"
    )
    args = parser.parse_args()

    print("Loading producer logs...")
    producer_df = load_producer_logs(args.producer_logs)
    print(f"  {len(producer_df)} files found")

    print("Loading consumer logs...")
    consumer_df = load_consumer_logs(args.consumer_logs)
    print(f"  {len(consumer_df)} files found")

    merged = producer_df.merge(consumer_df, on="filename", how="outer")
    merged = merged.sort_values("produced_start", na_position="last").reset_index(
        drop=True
    )

    if args.girder_api_url and args.topic_name:
        print("Checking Girder...")
        try:
            girder_files = get_girder_filenames(
                args.girder_api_url, args.girder_api_key, args.topic_name
            )
            merged["on_girder"] = merged["filename"].isin(girder_files)
            print(f"  {len(girder_files)} files found on Girder")
        except Exception as e:
            print(f"  Warning: Girder check failed: {e}", file=sys.stderr)
            merged["on_girder"] = False
    else:
        merged["on_girder"] = False

    merged = generate_report(merged, args.report)
    generate_png(merged, args.png)


if __name__ == "__main__":
    main()
