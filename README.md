# ETL Pipeline — Amman Digital Market

[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)

## Overview
This project implements a robust ETL (Extract, Transform, Load) pipeline using **Python** and **Pandas**. The pipeline extracts sales, product, and customer data from a **PostgreSQL** database, performs data cleaning and transformation to generate a customer-level analytics summary, and finally loads the results back into the database and a CSV file.

## Setup

1. **Start PostgreSQL container:**
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine

2. Load schema and data:
   ```bash
   docker exec -i postgres-m3-int psql -U postgres -d amman_market < schema.sql
   docker exec -i postgres-m3-int psql -U postgres -d amman_market < seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

The pipeline generates a file named customer_analytics.csv in the output/ directory. This file includes:

customer_id & customer_name: Primary customer identification.

total_orders: The number of unique orders placed by the customer.

total_revenue: Total spending after filtering.

avg_order_value: Average revenue per order.

## Quality Checks

To ensure data integrity, the following validations are performed:

Exclude Cancelled Orders: Only successful transactions are included to ensure accurate revenue reporting.

Filter Suspicious Quantities: Orders with quantities greater than 100 are excluded to prevent data entry errors.

Null Value Validation: The pipeline ensures no customer_id or customer_name fields are null.

Consistency Check: Verifies that total revenue and order counts are positive.

---

## License

MIT License

Copyright (c) 2026 Hashem Al-Qurashi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.