use std::sync::Arc;
use std::collections::HashMap;
use dotenv::dotenv;
use std::env;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use iceberg::io::FileIOBuilder;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenv().ok();

    // Get namespace and table name from command line arguments or use defaults
    let args: Vec<String> = env::args().collect();
    
    let (namespace, table_name, limit) = if args.len() > 3 {
        (args[1].clone(), args[2].clone(), args[3].parse::<usize>().unwrap_or(10))
    } else if args.len() > 2 {
        (args[1].clone(), args[2].clone(), 10)
    } else if args.len() > 1 {
        // If only one argument is provided, assume it's the table name in the default namespace
        ("my_namespace".to_string(), args[1].clone(), 10)
    } else {
        // Default values
        ("my_namespace".to_string(), "my_table".to_string(), 10)
    };

    // Set up AWS credentials from environment
    let access_key = env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let secret_key = env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    let region = env::var("S3_REGION").expect("S3_REGION must be set");
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");
    let path = env::var("S3_PATH").expect("S3_PATH must be set");

    // Set AWS environment variables
    env::set_var("AWS_ACCESS_KEY_ID", &access_key);
    env::set_var("AWS_SECRET_ACCESS_KEY", &secret_key);
    env::set_var("AWS_REGION", &region);

    // Create FileIO for S3
    let mut properties = HashMap::new();
    properties.insert("access-key-id".to_string(), access_key);
    properties.insert("secret-access-key".to_string(), secret_key);
    properties.insert("region".to_string(), region.clone());
    let _file_io = FileIOBuilder::new("s3").with_props(properties).build()?;

    // Create catalog
    let mut catalog_props = HashMap::new();
    catalog_props.insert("warehouse".to_string(), format!("s3://{}/{}", bucket, path));
    let catalog_config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_string())
        .props(catalog_props)
        .build();
    let catalog = RestCatalog::new(catalog_config);

    // Create DataFusion session
    let state = SessionStateBuilder::new().with_default_features().build();
    let catalog_provider = Arc::new(IcebergCatalogProvider::try_new(Arc::new(catalog)).await?);
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("my_catalog", catalog_provider);

    // Try to query the specified table with a limit
    println!("Contents of my_catalog.{}.{} (limited to {} rows):", namespace, table_name, limit);
    let query = format!("SELECT * FROM my_catalog.{}.{} LIMIT {}", namespace, table_name, limit);
    let df = ctx.sql(&query).await?;
    
    // Print the results
    df.show().await?;

    Ok(())
} 