# Gold layer – Star schema

This folder builds the reporting layer from the verified Silver Delta tables in
`adb_ecom_dev_01.silver`. Outputs are managed Delta tables in
`adb_ecom_dev_01.gold`.

## Run order

1. Run `NB_06_Dimension_Tables.sql`.
2. Run `NB_07_Fact_Tables.sql`.

Both notebooks use Delta `MERGE` operations keyed by business grain, so rerunning
them updates existing rows and inserts new rows rather than appending duplicate
records. Run dimensions before facts so surrogate keys are available.

## Dimensions

| Table | Business key | Purpose |
|---|---|---|
| `dim_customer` | `customer_id` | Customer identity and location |
| `dim_product` | `product_id` | Product attributes and translated category |
| `dim_seller` | `seller_id` | Seller identity and location |
| `dim_date` | `date_key` | Calendar attributes for role-playing dates |

Dimension surrogate keys are deterministic SHA-256 hashes of their natural keys.
The dimensions represent current attributes only. SCD Type 1/2 history remains
deferred as documented in `04_SCD`.

## Facts and grain

| Table | Grain | Measures |
|---|---|---|
| `fact_orders` | One row per `order_id` | `order_count` |
| `fact_order_items` | One row per `order_id + order_item_id` | `price`, `freight_value`, `item_total`, `item_count` |
| `fact_order_payments` | One row per `order_id + payment_sequential` | `payment_value`, `payment_count` |
| `fact_order_reviews` | One row per `review_id + order_id` | `review_score`, `review_count` |

The item fact joins to customer, product and seller dimensions. Order-level,
payment-level and review-level facts remain separate to avoid multiplying
measures when one order has multiple items, payments or reviews.

Missing date values use the `-1` unknown member in `dim_date`. Other
dimension lookups are left joins; missing dimension matches remain NULL and
are surfaced by the validation query at the end of the fact notebook.

## Validation

The notebooks finish with row-count / distinct-grain checks. The fact notebook
also reports missing customer, product and seller dimension keys for order
items. Review these outputs after running in Databricks.

## Scope and impact

- Reads only the existing Silver tables; does not rewrite Bronze or Silver.
- Creates or incrementally merges tables only in the Gold schema.
- Does not add SCD history, pipeline orchestration, or Power BI assets.
- Assumes the Silver source columns and business keys defined in the Silver
  transformation notebook remain stable.
