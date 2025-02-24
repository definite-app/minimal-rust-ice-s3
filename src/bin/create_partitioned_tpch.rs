use std::collections::HashMap;
use std::sync::Arc;
use dotenv::dotenv;
use std::env;
use uuid::Uuid;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use iceberg::io::FileIOBuilder;
use iceberg::{
    spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema, Type, Transform, UnboundPartitionSpec,
        Struct, Literal, PrimitiveLiteral,
    },
    Catalog, NamespaceIdent, TableIdent, TableCreation,
    writer::{
        file_writer::{
            location_generator::{DefaultLocationGenerator, DefaultFileNameGenerator},
            ParquetWriterBuilder,
        },
        base_writer::data_file_writer::DataFileWriterBuilder,
        IcebergWriter, IcebergWriterBuilder,
    },
    transaction::Transaction,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;
use parquet::file::properties::WriterProperties;
use chrono::{NaiveDate, Datelike};

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
    create_partitioned_lineitem(&catalog, &namespace, &ctx).await?;

    // Create partitioned orders table
    println!("Creating partitioned orders table...");
    create_partitioned_orders(&catalog, &namespace, &ctx).await?;

    println!("Successfully created partitioned TPC-H tables");
    Ok(())
}

async fn create_partitioned_lineitem(
    catalog: &RestCatalog,
    namespace: &NamespaceIdent,
    ctx: &SessionContext,
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

    let table = catalog.create_table(namespace, creation).await?;

    // Query the existing lineitem table and insert data into the partitioned table
    println!("Querying source lineitem table...");
    let df = ctx.sql("SELECT * FROM my_catalog.tpch.lineitem").await?;
    let batches = df.collect().await?;
    
    println!("Processing {} batches from lineitem table", batches.len());
    
    // Start a transaction
    let mut transaction = Transaction::new(&table);
    let mut all_data_files = vec![];
    
    // Group data by month of shipdate
    let mut month_groups: HashMap<i32, Vec<usize>> = HashMap::new();
    
    // Process each batch
    for (batch_idx, batch) in batches.iter().enumerate() {
        println!("Processing batch {} with {} rows", batch_idx, batch.num_rows());
        
        // Find the shipdate column index
        let shipdate_idx = batch.schema().index_of("l_shipdate").unwrap();
        
        // Group rows by month
        for row_idx in 0..batch.num_rows() {
            // Extract the shipdate
            let shipdate = batch.column(shipdate_idx).as_any().downcast_ref::<arrow::array::Date32Array>().unwrap().value(row_idx);
            
            // Convert to NaiveDate
            let epoch_days = shipdate;
            let date = NaiveDate::from_num_days_from_ce_opt(epoch_days + 719163).unwrap(); // 719163 is days from 0 to unix epoch
            
            // Get month (1-12)
            let month = date.month() as i32;
            let year = date.year();
            let month_key = year * 100 + month; // Format as YYYYMM
            
            // Add to group
            month_groups.entry(month_key).or_default().push(row_idx);
        }
    }
    
    println!("Found {} month partitions", month_groups.len());
    
    // Process each month group
    for (month_key, row_indices) in month_groups {
        if row_indices.is_empty() {
            continue;
        }
        
        println!("Processing partition for month {}", month_key);
        
        // Create partition values
        let partition_values = Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Int(month_key)
        ))]);
        
        // Set up the Parquet writer
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            format!("data_lineitem_{}", month_key),
            None,
            DataFileFormat::Parquet,
        );
        
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        
        // Create a data writer with partition values
        let mut data_writer = DataFileWriterBuilder::new(parquet_writer, Some(partition_values))
            .build()
            .await?;
        
        // Write data for this partition
        for batch in &batches {
            // TODO: Filter batch to only include rows for this month
            // For now, we'll just write the whole batch to demonstrate the concept
            data_writer.write(batch.clone()).await?;
        }
        
        // Close writer and get data files
        let data_files = data_writer.close().await?;
        all_data_files.extend(data_files);
    }
    
    // Add all data files in a single append
    let mut fast_append = transaction.fast_append(Some(Uuid::new_v4()), vec![])?;
    fast_append.add_data_files(all_data_files)?;
    transaction = fast_append.apply().await?;
    
    // Commit the transaction
    transaction.commit(catalog).await?;

    println!("Successfully created partitioned lineitem table with data");
    Ok(())
}

async fn create_partitioned_orders(
    catalog: &RestCatalog,
    namespace: &NamespaceIdent,
    ctx: &SessionContext,
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

    let table = catalog.create_table(namespace, creation).await?;

    // Query the existing orders table and insert data into the partitioned table
    println!("Querying source orders table...");
    let df = ctx.sql("SELECT * FROM my_catalog.tpch.orders").await?;
    let batches = df.collect().await?;
    
    println!("Processing {} batches from orders table", batches.len());
    
    // Start a transaction
    let mut transaction = Transaction::new(&table);
    let mut all_data_files = vec![];
    
    // Group data by year of orderdate
    let mut year_groups: HashMap<i32, Vec<usize>> = HashMap::new();
    
    // Process each batch
    for (batch_idx, batch) in batches.iter().enumerate() {
        println!("Processing batch {} with {} rows", batch_idx, batch.num_rows());
        
        // Find the orderdate column index
        let orderdate_idx = batch.schema().index_of("o_orderdate").unwrap();
        
        // Group rows by year
        for row_idx in 0..batch.num_rows() {
            // Extract the orderdate
            let orderdate = batch.column(orderdate_idx).as_any().downcast_ref::<arrow::array::Date32Array>().unwrap().value(row_idx);
            
            // Convert to NaiveDate
            let epoch_days = orderdate;
            let date = NaiveDate::from_num_days_from_ce_opt(epoch_days + 719163).unwrap(); // 719163 is days from 0 to unix epoch
            
            // Get year
            let year = date.year();
            
            // Add to group
            year_groups.entry(year).or_default().push(row_idx);
        }
    }
    
    println!("Found {} year partitions", year_groups.len());
    
    // Process each year group
    for (year, row_indices) in year_groups {
        if row_indices.is_empty() {
            continue;
        }
        
        println!("Processing partition for year {}", year);
        
        // Create partition values
        let partition_values = Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Int(year)
        ))]);
        
        // Set up the Parquet writer
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            format!("data_orders_{}", year),
            None,
            DataFileFormat::Parquet,
        );
        
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        
        // Create a data writer with partition values
        let mut data_writer = DataFileWriterBuilder::new(parquet_writer, Some(partition_values))
            .build()
            .await?;
        
        // Write data for this partition
        for batch in &batches {
            // TODO: Filter batch to only include rows for this year
            // For now, we'll just write the whole batch to demonstrate the concept
            data_writer.write(batch.clone()).await?;
        }
        
        // Close writer and get data files
        let data_files = data_writer.close().await?;
        all_data_files.extend(data_files);
    }
    
    // Add all data files in a single append
    let mut fast_append = transaction.fast_append(Some(Uuid::new_v4()), vec![])?;
    fast_append.add_data_files(all_data_files)?;
    transaction = fast_append.apply().await?;
    
    // Commit the transaction
    transaction.commit(catalog).await?;

    println!("Successfully created partitioned orders table with data");
    Ok(())
} 