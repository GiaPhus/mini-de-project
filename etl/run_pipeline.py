import argparse
from pathlib import Path
import json
import pandas as pd


def main(run_date: str, input_dir: str, output_dir: str) -> None:
    input_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    orders_file = input_path / f"orders_{run_date}.csv"
    items_file = input_path / f"order_items_{run_date}.csv"

    if not orders_file.exists():
        raise FileNotFoundError(f"Missing input file: {orders_file}")
    if not items_file.exists():
        raise FileNotFoundError(f"Missing input file: {items_file}")

    orders = pd.read_csv(orders_file)
    items = pd.read_csv(items_file)

    # Candidate will implement:
    # - Parse ingested_at as datetime
    # - Dedup orders by order_id keep latest ingested_at
    # - Validate items: quantity not null, unit_price > 0
    # - Reject orphan items (items.order_id not found in orders)
    # - Compute daily revenue for completed orders
    # - Write output files and quality_report.json

    (out_path / "daily_revenue.csv").write_text("order_date,total_revenue,orders_count\n")
    report = {
        "run_date": run_date,
        "input": {"orders": int(len(orders)), "order_items": int(len(items))},
        "note": "TODO: implement pipeline logic"
    }
    with open(out_path / "quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD (e.g. 2024-01-01)")
    parser.add_argument("--input-dir", default="data")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()
    main(args.run_date, args.input_dir, args.output_dir)
