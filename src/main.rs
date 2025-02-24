use std::collections::HashMap;
use std::sync::Arc;
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
    datatypes::SchemaRef,
};
use parquet::file::properties::WriterProperties;
use iceberg::{
    io::FileIOBuilder,
    spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema, Type, Transform, UnboundPartitionSpec,
        Struct, Literal, PrimitiveLiteral,
    },
    Catalog, NamespaceIdent, TableIdent, TableCreation,
    arrow::schema_to_arrow_schema,
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
use std::env;
use dotenv::dotenv;
use uuid::Uuid;

mod duckdb_tpch;

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

    // Create namespace if it doesn't exist
    let namespace = NamespaceIdent::new("my_namespace".to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    // Create table identifier
    let table_ident = TableIdent::new(namespace.clone(), "my_table".to_string());

    // Drop table if it exists
    if catalog.table_exists(&table_ident).await? {
        catalog.drop_table(&table_ident).await?;
    }

    // Create schema for the table
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(2, "name", Type::Primitive(PrimitiveType::String))),
        ])
        .build()?;

    // Create partition spec
    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "name", Transform::Identity)?
        .build();

    let partition_spec = unbound_partition_spec.bind(schema.clone())?;

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

    let table = catalog.create_table(&namespace, creation).await?;

    // Start a transaction
    let mut transaction = Transaction::new(&table);

    // Sample data and partitions
    let data = vec![
        ("Alice", 1),
        ("Bob", 2),
        ("Charlie", 3),
    ];

    // Collect all data files
    let mut all_data_files = vec![];

    // Write each partition separately
    for (name, id) in data {
        // Create data for this partition
        let id_array = Int32Array::from(vec![id]);
        let name_array = StringArray::from(vec![name]);
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let batch = RecordBatch::try_new(
            SchemaRef::new(arrow_schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )?;

        // Set up the Parquet writer
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(format!("data_{}", name), None, DataFileFormat::Parquet);
        
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        // Create partition values
        let partition_values = Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::String(name.to_string())
        ))]);

        // Create a data writer with partition values
        let mut data_writer = DataFileWriterBuilder::new(parquet_writer, Some(partition_values))
            .build()
            .await?;

        // Write the data
        data_writer.write(batch).await?;
        let data_files = data_writer.close().await?;
        all_data_files.extend(data_files);
    }

    // Add all data files in a single append
    let mut fast_append = transaction.fast_append(Some(Uuid::new_v4()), vec![])?;
    fast_append.add_data_files(all_data_files)?;
    transaction = fast_append.apply().await?;

    // Commit the transaction
    transaction.commit(&catalog).await?;

    println!("Successfully created partitioned table with sample data");
    
    // Create TPCH tables
    let warehouse_path = format!("s3://{}/{}/tpch", bucket, path);
    duckdb_tpch::create_tpch_tables(&catalog, &warehouse_path, 0.1).await?;
    
    println!("Successfully created TPCH tables");
    Ok(())
}