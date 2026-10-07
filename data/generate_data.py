"""CommerceFlow synthetic data generator.

Creates customers, products and orders for one run date,
plus a _SUCCESS manifest written last.

A small percentage of records are intentionally invalid so that
the CommerceFlow validation and quarantine pipeline has realistic
bad data to process.

Usage:
    python data/generate_data.py --date 2026-10-07 --orders 100000
"""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker


CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Books",
    "Beauty",
    "Sports",
    "Toys",
]

ORDER_STATUSES = [
    "PLACED",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Dataset run date, YYYY-MM-DD",
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=10_000,
        help="Number of customers",
    )

    parser.add_argument(
        "--products",
        type=int,
        default=500,
        help="Number of products",
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=100_000,
        help="Number of orders",
    )

    parser.add_argument(
        "--bad-rate",
        type=float,
        default=0.02,
        help="Percentage of intentionally bad order records",
    )

    parser.add_argument(
        "--out",
        default="data/output",
        help="Output directory",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data",
    )

    args = parser.parse_args()

    if not 0 <= args.bad_rate <= 1:
        raise ValueError("--bad-rate must be between 0 and 1")

    random.seed(args.seed)
    Faker.seed(args.seed)

    fake = Faker("en_IN")

    run_day = date.fromisoformat(args.date)

    output_dir = Path(args.out) / f"dt={args.date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # Customers
    # ---------------------------------------------------------

    customers = []

    for i in range(1, args.customers + 1):
        customers.append(
            [
                f"C{i:06d}",
                fake.name(),
                fake.email(),
                fake.city(),
                fake.state(),
                fake.date_between(
                    start_date="-3y",
                    end_date="-30d",
                ).isoformat(),
            ]
        )

    # ---------------------------------------------------------
    # Products
    # ---------------------------------------------------------

    products = []

    for i in range(1, args.products + 1):
        products.append(
            [
                f"P{i:05d}",
                fake.catch_phrase()[:40],
                random.choice(CATEGORIES),
                round(random.uniform(50, 5000), 2),
            ]
        )

    # ---------------------------------------------------------
    # Orders
    # ---------------------------------------------------------

    orders = []

    for i in range(1, args.orders + 1):
        product_id = f"P{random.randint(1, args.products):05d}"

        # Find the product price.
        product_index = int(product_id[1:]) - 1
        product_price = products[product_index][3]

        quantity = random.randint(1, 5)

        orders.append(
            [
                f"O{i:08d}",
                f"C{random.randint(1, args.customers):06d}",
                product_id,
                quantity,
                product_price,
                random.choice(ORDER_STATUSES),
                (
                    run_day
                    - timedelta(days=random.randint(0, 90))
                ).isoformat(),
            ]
        )

    # ---------------------------------------------------------
    # Inject bad order records
    # ---------------------------------------------------------

    bad_count = int(len(orders) * args.bad_rate)

    bad_indexes = random.sample(
        range(len(orders)),
        bad_count,
    )

    bad_types = [
        "negative_quantity",
        "zero_quantity",
        "missing_customer",
        "orphan_customer",
        "orphan_product",
        "future_date",
        "duplicate_order",
        "negative_unit_price",
    ]

    for idx in bad_indexes:

        bad_type = random.choice(bad_types)
        row = orders[idx]

        if bad_type == "negative_quantity":
            row[3] = -random.randint(1, 5)

        elif bad_type == "zero_quantity":
            row[3] = 0

        elif bad_type == "missing_customer":
            row[1] = ""

        elif bad_type == "orphan_customer":
            row[1] = "C999999"

        elif bad_type == "orphan_product":
            row[2] = "P99999"

        elif bad_type == "future_date":
            row[6] = (
                run_day + timedelta(days=random.randint(1, 60))
            ).isoformat()

        elif bad_type == "duplicate_order" and idx > 0:
            row[0] = orders[idx - 1][0]

        elif bad_type == "negative_unit_price":
            row[4] = -round(random.uniform(10, 500), 2)

    # ---------------------------------------------------------
    # Inject bad product records
    # ---------------------------------------------------------

    bad_product_count = max(
        1,
        len(products) // 100,
    )

    for idx in random.sample(
        range(len(products)),
        bad_product_count,
    ):
        products[idx][3] = -10.0

    # ---------------------------------------------------------
    # Inject bad customer records
    # ---------------------------------------------------------

    bad_customer_count = max(
        1,
        len(customers) // 100,
    )

    for idx in random.sample(
        range(len(customers)),
        bad_customer_count,
    ):
        customers[idx][2] = "not-an-email"

    # ---------------------------------------------------------
    # Write datasets
    # ---------------------------------------------------------

    write_csv(
        output_dir / "customers.csv",
        [
            "customer_id",
            "name",
            "email",
            "city",
            "state",
            "registration_date",
        ],
        customers,
    )

    write_csv(
        output_dir / "products.csv",
        [
            "product_id",
            "product_name",
            "category",
            "price",
        ],
        products,
    )

    write_csv(
        output_dir / "orders.csv",
        [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_status",
            "order_date",
        ],
        orders,
    )

    # ---------------------------------------------------------
    # Manifest
    # ---------------------------------------------------------

    # IMPORTANT:
    # _SUCCESS is written LAST.
    # Later, S3/EventBridge will use this as the dataset-ready signal.

    (output_dir / "_SUCCESS").write_text(
        "CommerceFlow dataset ready\n",
        encoding="utf-8",
    )

    print()
    print("CommerceFlow dataset generated successfully")
    print("-------------------------------------------")
    print(f"Run date       : {args.date}")
    print(f"Customers      : {len(customers):,}")
    print(f"Products       : {len(products):,}")
    print(f"Orders         : {len(orders):,}")
    print(f"Bad order rows : {bad_count:,}")
    print(f"Output         : {output_dir}")
    print()


def write_csv(path, header, rows):
    """Write rows to a CSV file."""

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(header)
        writer.writerows(rows)


if __name__ == "__main__":
    main()