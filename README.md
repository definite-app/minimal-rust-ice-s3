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
cargo run --bin read_table
```

This will:
- Connect to the same table
- Execute a SELECT query
- Display the results

## Project Structure

- `src/main.rs` - Main program for writing data
- `src/bin/read_table.rs` - Example of reading data
- `docker-compose.yml` - REST catalog server configuration
- `.env` - Environment variables (not tracked in git)

## Configuration

The project uses the following configuration:

- REST Catalog Server: `http://localhost:8181`
- S3 Storage: Configured via environment variables
- Table Location: `s3://${S3_BUCKET}/${S3_PATH}`
- Table Schema:
  - `id` (Int32)
  - `name` (String)

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

## License

This project is licensed under the Apache License 2.0. 