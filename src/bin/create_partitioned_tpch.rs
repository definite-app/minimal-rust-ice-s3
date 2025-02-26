use std::collections::HashMap;
use std::sync::Arc;
use dotenv::dotenv;
use std::env;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use iceberg::io::FileIOBuilder;
use iceberg::{
    spec::{
        NestedField, PrimitiveType, Schema, Type, Transform, UnboundPartitionSpec,
    },
    Catalog, NamespaceIdent, TableIdent, TableCreation,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenv().ok();

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

    // Create a new catalog for DataFusion
    let mut catalog_props = HashMap::new();
    catalog_props.insert("warehouse".to_string(), format!("s3://{}/{}", bucket, path));
    let catalog_config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_string())
        .props(catalog_props)
        .build();
    let df_catalog = RestCatalog::new(catalog_config);

    // Create DataFusion session to read from existing tables
    let state = SessionStateBuilder::new().with_default_features().build();
    let catalog_provider = Arc::new(IcebergCatalogProvider::try_new(Arc::new(df_catalog)).await?);
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("my_catalog", catalog_provider);

    // Create a new namespace for partitioned tables
    let namespace = NamespaceIdent::new("tpch_partitioned".to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    // Create partitioned lineitem table
    println!("Creating partitioned lineitem table...");
    create_partitioned_lineitem(&catalog, &namespace).await?;

    // Create partitioned orders table
    println!("Creating partitioned orders table...");
    create_partitioned_orders(&catalog, &namespace).await?;

    println!("Successfully created partitioned TPC-H tables");
    println!("Note: Tables are created without data. To populate them, you'll need to run a separate process.");
    Ok(())
}

async fn create_partitioned_lineitem(
    catalog: &RestCatalog,
    namespace: &NamespaceIdent,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define table identifier
    let table_name = "lineitem";
    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    // Drop table if it exists
    if catalog.table_exists(&table_ident).await? {
        catalog.drop_table(&table_ident).await?;
    }

    // Create schema for lineitem table
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(1, "l_orderkey", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(2, "l_partkey", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(3, "l_suppkey", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(4, "l_linenumber", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(5, "l_quantity", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(6, "l_extendedprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(7, "l_discount", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(8, "l_tax", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(9, "l_returnflag", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(10, "l_linestatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(11, "l_shipdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(12, "l_commitdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(13, "l_receiptdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(14, "l_shipinstruct", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(15, "l_shipmode", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(16, "l_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build()?;

    // Create partition spec - partition by month of shipdate
    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(11, "shipdate_month", Transform::Month)?
        .build();

    let partition_spec = unbound_partition_spec.bind(schema.clone())?;

    // Create table properties
    let mut properties = HashMap::new();
    properties.insert("write.format.default".to_string(), "parquet".to_string());
    properties.insert("write.metadata.compression-codec".to_string(), "none".to_string());

    // Create the table with partition spec
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec)
        .properties(properties)
        .build();

    let _table = catalog.create_table(namespace, creation).await?;
    
    println!("Successfully created partitioned lineitem table schema");
    
    Ok(())
}

async fn create_partitioned_orders(
    catalog: &RestCatalog,
    namespace: &NamespaceIdent,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define table identifier
    let table_name = "orders";
    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    // Drop table if it exists
    if catalog.table_exists(&table_ident).await? {
        catalog.drop_table(&table_ident).await?;
    }

    // Create schema for orders table
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(1, "o_orderkey", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(2, "o_custkey", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(3, "o_orderstatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(4, "o_totalprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::required(5, "o_orderdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::required(6, "o_orderpriority", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(7, "o_clerk", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(8, "o_shippriority", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(9, "o_comment", Type::Primitive(PrimitiveType::String))),
        ])
        .build()?;

    // Create partition spec - partition by year of orderdate
    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(5, "orderdate_year", Transform::Year)?
        .build();

    let partition_spec = unbound_partition_spec.bind(schema.clone())?;

    // Create table properties
    let mut properties = HashMap::new();
    properties.insert("write.format.default".to_string(), "parquet".to_string());
    properties.insert("write.metadata.compression-codec".to_string(), "none".to_string());

    // Create the table with partition spec
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec)
        .properties(properties)
        .build();

    let _table = catalog.create_table(namespace, creation).await?;
    
    println!("Successfully created partitioned orders table schema");
    
    Ok(())
} 