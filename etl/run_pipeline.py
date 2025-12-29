import argparse
from pathlib import Path
import json
import pandas as pd


def stage_and_validate_orders(orders: pd.DataFrame):
    required_cols = ["order_id", "customer_id", "order_date", "status"]

    for col in required_cols:
        if col not in orders.columns:
            orders[col] = None

    if "ingested_at" in orders.columns:
        orders["ingested_at"] = pd.to_datetime(
            orders["ingested_at"], errors="coerce"
        )

    orders["order_date"] = pd.to_datetime(
        orders["order_date"], errors="coerce"
    )

    dq_mask = orders[required_cols].isna().any(axis=1)

    rejected_orders = orders[dq_mask].copy()
    valid_stage = orders[~dq_mask].copy()

    if "ingested_at" in valid_stage.columns:
        valid_orders = (
            valid_stage
            .sort_values("ingested_at")
            .drop_duplicates(subset=["order_id"], keep="last")
        )
    else:
        valid_orders = valid_stage.drop_duplicates(
            subset=["order_id"], keep="first"
        )

    orders_after_dq = len(valid_stage)
    orders_rejected_dq = len(rejected_orders)
    orders_deduped = orders_after_dq - len(valid_orders)

    return (
        valid_orders,
        rejected_orders,
        orders_after_dq,
        orders_rejected_dq,
        orders_deduped,
    )



def stage_and_validate_items(items: pd.DataFrame, valid_orders: pd.DataFrame):
    required_cols = ["order_id", "quantity", "unit_price"]

    for col in required_cols:
        if col not in items.columns:
            items[col] = None

    if "ingested_at" in items.columns:
        items["ingested_at"] = pd.to_datetime(
            items["ingested_at"], errors="coerce"
        )

    dq_mask = (
        items["quantity"].isna()
        | items["unit_price"].isna()
        | (items["unit_price"] <= 0)
    )

    rejected_dq = items[dq_mask].copy()
    valid_stage = items[~dq_mask].copy()

    valid_order_ids = set(valid_orders["order_id"])
    orphan_mask = ~valid_stage["order_id"].isin(valid_order_ids)

    rejected_orphan = valid_stage[orphan_mask].copy()
    valid_items = valid_stage[~orphan_mask].copy()

    items_after_dq = len(valid_stage)
    items_rejected_dq = len(rejected_dq)
    items_rejected_orphan = len(rejected_orphan)

    rejected_items = pd.concat(
        [rejected_dq, rejected_orphan], ignore_index=True
    )

    return (
        valid_items,
        rejected_items,
        items_after_dq,
        items_rejected_dq,
        items_rejected_orphan,
    )



def compute_daily_revenue(valid_orders: pd.DataFrame, valid_items: pd.DataFrame):
    completed_orders = valid_orders[
        valid_orders["status"].str.strip().str.lower() == "completed"
    ]

    completed_orders_count = len(completed_orders)

    if completed_orders.empty or valid_items.empty:
        return (
            pd.DataFrame(columns=["order_date", "total_revenue", "orders_count"]),
            completed_orders_count,
            0,
            0.0,
        )

    joined = valid_items.merge(
        completed_orders[["order_id", "order_date"]],
        on="order_id",
        how="inner"
    )

    joined["amount"] = joined["quantity"] * joined["unit_price"]

    daily_revenue = (
        joined
        .groupby("order_date")
        .agg(
            total_revenue=("amount", "sum"),
            orders_count=("order_id", "nunique"),
        )
        .reset_index()
    )

    orders_with_revenue = int(daily_revenue["orders_count"].sum())
    total_revenue = float(daily_revenue["total_revenue"].sum())

    return (
        daily_revenue,
        completed_orders_count,
        orders_with_revenue,
        total_revenue,
    )



def build_quality_report(
    run_date: str,
    input_orders: int,
    input_items: int,
    orders_after_dq: int,
    orders_rejected_dq: int,
    orders_deduped: int,
    items_after_dq: int,
    items_rejected_dq: int,
    items_rejected_orphan: int,
    completed_orders: int,
    orders_with_revenue: int,
    total_revenue: float,
):
    return {
        "run_date": run_date,
        "input": {
            "orders": input_orders,
            "order_items": input_items,
        },
        "orders": {
            "after_dq": orders_after_dq,
            "rejected_dq": orders_rejected_dq,
            "deduplicated": orders_deduped,
            "final_valid": orders_after_dq - orders_deduped,
        },
        "order_items": {
            "after_dq": items_after_dq,
            "rejected_dq": items_rejected_dq,
            "rejected_orphan": items_rejected_orphan,
            "final_valid": items_after_dq - items_rejected_orphan,
        },
        "business": {
            "completed_orders": completed_orders,
            "orders_with_revenue": orders_with_revenue,
            "total_revenue": total_revenue,
        },
    }



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

    valid_orders, rejected_orders, orders_after_dq, orders_rejected_dq, orders_deduped = (
    stage_and_validate_orders(orders)
    )
    (
        valid_items,
        rejected_items,
        items_after_dq,
        items_rejected_dq,
        items_rejected_orphan,
    ) = stage_and_validate_items(items, valid_orders)

    (
        daily_revenue,
        completed_orders,
        orders_with_revenue,
        total_revenue,
    ) = compute_daily_revenue(valid_orders, valid_items)

    daily_revenue.to_csv(out_path / "daily_revenue.csv", index=False)

    rejected_orders.to_csv(out_path / "reject_orders.csv", index=False)

    rejected_items.to_csv(out_path / "rejected_items.csv", index=False)

    report = build_quality_report(
    run_date,
    len(orders),
    len(items),
    orders_after_dq,
    orders_rejected_dq,
    orders_deduped,
    items_after_dq,
    items_rejected_dq,
    items_rejected_orphan,
    completed_orders,
    orders_with_revenue,
    total_revenue,
    )


    with open(out_path / "quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD (e.g. 2024-01-01)")
    parser.add_argument("--input-dir", default="data")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()
    main(args.run_date, args.input_dir, args.output_dir)
