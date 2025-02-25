# Minimal Rust Iceberg S3 Example

This repository demonstrates how to use Apache Iceberg with Rust and AWS S3 storage. It includes examples of writing data to and reading data from Iceberg tables using the REST catalog.

## Prerequisites

- Rust (latest stable version)
- Docker
- AWS Account with S3 access
- Git

## Setup

1. Clone the repository:
```bash
git clone https://github.com/definite-app/minimal-rust-ice-s3.git
cd minimal-rust-ice-s3
```

2. Set up your environment variables:
```bash
# Copy the example .env file
cp .env.example .env

# Edit .env with your AWS credentials and S3 configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=your_aws_region
S3_BUCKET=your_bucket_name
S3_PATH=your_path
```

3. Start the REST catalog server:
```bash
docker compose up -d
```

## Usage

### Writing Data

The project includes an example that writes sample data to an Iceberg table:

```bash
cargo run --bin mjr
```

This will:
- Create a namespace if it doesn't exist
- Create a table with a simple schema (id: Int, name: String)
- Write sample records to the table

### Reading Data

To read the data back from the table:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin read_table
```

This will:
- Connect to the same table
- Execute a SELECT query
- Display the results

### Listing Tables

To list available tables in a namespace:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin list_tables
```

This will:
- Connect to the catalog
- List tables in the default namespace
- Display sample data from the default table

### Reading Custom Tables

To read data from a specific table with a limit:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin read_custom_table <namespace> <table_name> <limit>
```

For example:
```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin read_custom_table tpch customer 10
```

This will:
- Connect to the specified table in the given namespace
- Execute a SELECT query with the specified limit
- Display the results

### Running Predefined Queries

To run a predefined query that joins tables:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_query
```

This will:
- Execute a default query that joins customer and nation tables
- Display the results

### Running Custom SQL Queries

To run any custom SQL query against your Iceberg tables:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "<SQL_QUERY>"
```

For example:
```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT l_shipmode, COUNT(*) as count FROM my_catalog.tpch.lineitem GROUP BY l_shipmode ORDER BY count DESC"
```

This will:
- Execute your custom SQL query against the Iceberg tables
- Display the results

## Example Queries

Here are some example complex queries you can run:

### Analyze shipping modes in lineitem table:
```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT l_shipmode, COUNT(*) as count, SUM(l_quantity) as total_quantity, AVG(l_extendedprice) as avg_price FROM my_catalog.tpch.lineitem GROUP BY l_shipmode ORDER BY count DESC"
```

### Analyze order data by region:
```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT r.r_name as region, COUNT(DISTINCT c.c_custkey) as customer_count, COUNT(o.o_orderkey) as order_count, SUM(o.o_totalprice) as total_sales FROM my_catalog.tpch.orders o JOIN my_catalog.tpch.customer c ON o.o_custkey = c.c_custkey JOIN my_catalog.tpch.nation n ON c.c_nationkey = n.n_nationkey JOIN my_catalog.tpch.region r ON n.n_regionkey = r.r_regionkey GROUP BY r.r_name ORDER BY total_sales DESC"
```

### Find top customers by total purchase amount:
```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT c.c_name, n.n_name as nation, r.r_name as region, COUNT(o.o_orderkey) as order_count, SUM(o.o_totalprice) as total_spent FROM my_catalog.tpch.customer c JOIN my_catalog.tpch.orders o ON c.c_custkey = o.o_custkey JOIN my_catalog.tpch.nation n ON c.c_nationkey = n.n_nationkey JOIN my_catalog.tpch.region r ON n.n_regionkey = r.r_regionkey GROUP BY c.c_name, n.n_name, r.r_name ORDER BY total_spent DESC LIMIT 10"
```

## Partitioned Tables

The project includes functionality to create partitioned versions of the TPC-H tables for improved query performance:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin create_partitioned_tpch
```

This will:
- Create a new namespace called `tpch_partitioned`
- Create a partitioned version of the `lineitem` table (partitioned by month of shipdate)
- Create a partitioned version of the `orders` table (partitioned by year of orderdate)

Note: The tables are created without data. To populate them with data, you would need to implement a separate process to:
1. Read data from the original tables
2. Convert data types as needed (e.g., Int32 to Int64 for key fields)
3. Write the data to the partitioned tables with appropriate partition values

You can verify the table structures using:

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "DESCRIBE my_catalog.tpch_partitioned.lineitem"
```

```bash
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "DESCRIBE my_catalog.tpch_partitioned.orders"
```

Once populated, partitioning would improve query performance when filtering on the partition columns:

```bash
# Query using partition pruning on lineitem (example for when data is populated)
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT COUNT(*) FROM my_catalog.tpch_partitioned.lineitem WHERE l_shipdate BETWEEN DATE '1992-01-01' AND DATE '1992-12-31'"
```

```bash
# Query using partition pruning on orders (example for when data is populated)
OPENSSL_DIR=/opt/homebrew/opt/openssl@3 cargo run --bin run_custom_query "SELECT COUNT(*) FROM my_catalog.tpch_partitioned.orders WHERE o_orderdate >= DATE '1993-01-01'"
```

## Project Structure

- `src/main.rs` - Main program for writing data
- `src/bin/read_table.rs` - Example of reading data from the default table
- `src/bin/list_tables.rs` - Example of listing available tables
- `src/bin/read_custom_table.rs` - Example of reading data from a specific table
- `src/bin/run_query.rs` - Example of running a predefined query
- `src/bin/run_custom_query.rs` - Example of running custom SQL queries
- `src/bin/create_partitioned_tpch.rs` - Example of creating partitioned TPC-H tables
- `docker-compose.yml` - REST catalog server configuration
- `.env` - Environment variables (not tracked in git)

## Configuration

The project uses the following configuration:

- REST Catalog Server: `http://localhost:8181`
- S3 Storage: Configured via environment variables
- Table Location: `s3://${S3_BUCKET}/${S3_PATH}`
- Default Table Schema:
  - `id` (Int32)
  - `name` (String)
- TPC-H Tables: Available in the `tpch` namespace

## Development

1. Make sure Docker is running
2. Set up your environment variables
3. Start the REST catalog server
4. Run the examples

## Troubleshooting

1. If you see connection errors, ensure:
   - Docker is running
   - The REST catalog server is up (`docker compose ps`)
   - Your AWS credentials are correct

2. For S3 access issues:
   - Verify your AWS credentials
   - Check S3 bucket permissions
   - Ensure the bucket exists

3. For OpenSSL-related errors:
   - Make sure to include `OPENSSL_DIR=/opt/homebrew/opt/openssl@3` when running commands
   - On non-macOS systems, you may need to adjust this path
