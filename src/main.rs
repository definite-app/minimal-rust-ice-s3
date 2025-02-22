use std::collections::HashMap;
use std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::file::properties::WriterProperties;
use iceberg::{
    io::FileIOBuilder,
    spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema, Type,
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
    let file_io = FileIOBuilder::new("s3").with_props(properties).build()?;

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

    let mut properties = HashMap::new();
    properties.insert("write.format.default".to_string(), "parquet".to_string());
    properties.insert("write.metadata.compression-codec".to_string(), "none".to_string());

    // Create the table
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(schema.clone())
        .properties(properties)
        .build();

    let table = catalog.create_table(&namespace, creation).await?;

    // Start a transaction
    let mut transaction = Transaction::new(&table);

    // Create sample data
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )?;

    // Set up the Parquet writer
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen = DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
    
    let parquet_writer = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    // Create a data writer
    let mut data_writer = DataFileWriterBuilder::new(parquet_writer, None)
        .build()
        .await?;

    // Write the data
    data_writer.write(batch).await?;
    let data_files = data_writer.close().await?;

    // Add the data files to the transaction
    let mut fast_append = transaction.fast_append(Some(Uuid::new_v4()), vec![])?;
    fast_append.add_data_files(data_files)?;
    transaction = fast_append.apply().await?;

    // Commit the transaction
    transaction.commit(&catalog).await?;

    println!("Successfully created table with sample data");
    Ok(())
}