#!/usr/bin/env python3
"""
Example script demonstrating data aggregation with Transmog.
This example shows how to group and aggregate data for analysis.
"""

from transmog import Processor, AggregateOperation, GroupBy
import os


def main():
    # Initialize processor
    processor = Processor()

    # Create a more complex dataset for aggregation
    sales_data = [
        {
            "region": "North",
            "product": "Widget",
            "date": "2023-01-15",
            "units": 42,
            "revenue": 1260.00,
        },
        {
            "region": "South",
            "product": "Gadget",
            "date": "2023-01-20",
            "units": 18,
            "revenue": 720.00,
        },
        {
            "region": "North",
            "product": "Gadget",
            "date": "2023-01-22",
            "units": 25,
            "revenue": 1000.00,
        },
        {
            "region": "East",
            "product": "Widget",
            "date": "2023-01-25",
            "units": 30,
            "revenue": 900.00,
        },
        {
            "region": "West",
            "product": "Widget",
            "date": "2023-01-28",
            "units": 15,
            "revenue": 450.00,
        },
        {
            "region": "North",
            "product": "Widget",
            "date": "2023-02-05",
            "units": 38,
            "revenue": 1140.00,
        },
        {
            "region": "South",
            "product": "Gadget",
            "date": "2023-02-10",
            "units": 22,
            "revenue": 880.00,
        },
        {
            "region": "East",
            "product": "Widget",
            "date": "2023-02-12",
            "units": 28,
            "revenue": 840.00,
        },
        {
            "region": "West",
            "product": "Gadget",
            "date": "2023-02-15",
            "units": 35,
            "revenue": 1400.00,
        },
        {
            "region": "North",
            "product": "Gadget",
            "date": "2023-02-18",
            "units": 20,
            "revenue": 800.00,
        },
    ]

    # Create a temporary CSV file with the sales data
    os.makedirs("output", exist_ok=True)
    temp_csv = "output/sales_data.csv"
    processor.create_csv_from_records(sales_data, temp_csv)

    print(f"Created sample sales data in {temp_csv}")
    print("Performing various aggregation operations...")

    # Example 1: Group by region and calculate total sales
    region_sales = processor.aggregate_csv(
        temp_csv,
        group_by=GroupBy("region"),
        operations=[
            AggregateOperation("units", "total_units", "sum"),
            AggregateOperation("revenue", "total_revenue", "sum"),
        ],
    )

    # Example 2: Group by product and calculate average, min, max sales
    product_stats = processor.aggregate_csv(
        temp_csv,
        group_by=GroupBy("product"),
        operations=[
            AggregateOperation("units", "total_units", "sum"),
            AggregateOperation("units", "avg_units", "avg"),
            AggregateOperation("revenue", "avg_revenue", "avg"),
            AggregateOperation("revenue", "min_revenue", "min"),
            AggregateOperation("revenue", "max_revenue", "max"),
        ],
    )

    # Example 3: Group by multiple fields (region and product)
    region_product_sales = processor.aggregate_csv(
        temp_csv,
        group_by=GroupBy(["region", "product"]),
        operations=[
            AggregateOperation("units", "total_units", "sum"),
            AggregateOperation("revenue", "total_revenue", "sum"),
        ],
    )

    # Example 4: Extract month from date and group by month
    monthly_sales = processor.aggregate_csv(
        temp_csv,
        group_by=GroupBy("date", transform=lambda x: x[:7]),  # Extract YYYY-MM
        operations=[
            AggregateOperation("units", "monthly_units", "sum"),
            AggregateOperation("revenue", "monthly_revenue", "sum"),
        ],
    )

    # Save all aggregation results
    region_sales.write_csv("output/region_sales.csv")
    product_stats.write_csv("output/product_stats.csv")
    region_product_sales.write_csv("output/region_product_sales.csv")
    monthly_sales.write_csv("output/monthly_sales.csv")

    # Print some sample results
    print("\nSales by Region:")
    for record in region_sales.records:
        print(
            f"  {record['region']}: {record['total_units']} units, ${record['total_revenue']:.2f}"
        )

    print("\nSales by Product (with statistics):")
    for record in product_stats.records:
        print(
            f"  {record['product']}: {record['total_units']} units, ${record['avg_revenue']:.2f} avg"
        )

    print("\nSales by Month:")
    for record in monthly_sales.records:
        print(
            f"  {record['date']}: {record['monthly_units']} units, ${record['monthly_revenue']:.2f}"
        )

    print("\nAll aggregated data saved to output directory.")


if __name__ == "__main__":
    main()
