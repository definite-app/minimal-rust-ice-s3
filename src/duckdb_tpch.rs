use anyhow::Result as AnyhowResult;
use duckdb::Connection;
use iceberg::{
    spec::{
        Schema, NestedField, Type, PrimitiveType, DataFileFormat,
    },
    Catalog, TableCreation, NamespaceIdent, TableIdent,
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
use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use parquet::{
    file::{
        properties::WriterProperties,
        reader::{SerializedFileReader, FileReader},
    },
    record::RowAccessor,
};
use std::fs::File;
use uuid::Uuid;
use arrow::{
    record_batch::RecordBatch,
    array::{
        Int32Array, Int64Array, StringArray, Date32Array,
        Decimal128Array, ArrayRef,
    },
    datatypes::SchemaRef,
};
use chrono::{NaiveDate, Datelike};

pub async fn create_tpch_tables(catalog: &impl Catalog, _warehouse_path: &str, scale_factor: f64) -> AnyhowResult<()> {
    // Initialize DuckDB with TPCH extension
    let conn = Connection::open_in_memory()?;
    
    // Load TPCH extension and generate data
    conn.execute_batch(&format!("
        INSTALL tpch;
        LOAD tpch;
        CALL dbgen(sf={});
    ", scale_factor))?;

    // Create TPCH namespace if it doesn't exist
    let namespace = NamespaceIdent::new("tpch".to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    // List of TPCH tables to create
    let tables = vec![
        "lineitem", "orders", "customer", "part",
        "partsupp", "supplier", "nation", "region"
    ];

    for table_name in tables {
        println!("Creating table: {}", table_name);
        
        // Drop table if it exists
        let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());
        if catalog.table_exists(&table_ident).await? {
            catalog.drop_table(&table_ident).await?;
        }
        
        // Create Iceberg schema for the table
        let schema = create_tpch_schema(table_name).await?;
        
        // Create Iceberg table
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema.clone())
            .properties({
                let mut props = HashMap::new();
                props.insert("write.format.default".to_string(), "parquet".to_string());
                props.insert("write.metadata.compression-codec".to_string(), "none".to_string());
                props
            })
            .build();
        
        let table = catalog.create_table(&namespace, creation).await?;

        // Export DuckDB table to Parquet
        let temp_parquet = format!("temp_{}.parquet", Uuid::new_v4());
        conn.execute(&format!(
            "COPY {} TO '{}' (FORMAT PARQUET);",
            table_name, temp_parquet
        ), [])?;

        println!("Exported {} to {}", table_name, temp_parquet);

        // Read the Parquet file
        let file = File::open(&temp_parquet)?;
        let reader = SerializedFileReader::new(file)?;
        let arrow_schema = schema_to_arrow_schema(&schema)?;

        // Set up the Parquet writer for Iceberg
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_gen = DefaultFileNameGenerator::new(
            format!("data_{}", table_name),
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

        // Create a data writer
        let mut data_writer = DataFileWriterBuilder::new(parquet_writer, None)
            .build()
            .await?;

        // Read rows and convert to RecordBatch
        let mut row_iter = reader.get_row_iter(None)?;
        let mut rows = Vec::new();
        while let Some(row) = row_iter.next() {
            let row = row?;
            rows.push(row);
        }

        // Check for null values in required fields
        println!("Checking for null values in required fields for table: {}", table_name);
        let mut has_null_required_fields = false;
        for (i, field) in schema.as_struct().fields().iter().enumerate() {
            if field.required {
                let null_count = rows.iter().filter(|row| {
                    match field.field_type.as_ref() {
                        Type::Primitive(PrimitiveType::Int) => row.get_int(i).is_err(),
                        Type::Primitive(PrimitiveType::Long) => row.get_long(i).is_err(),
                        Type::Primitive(PrimitiveType::String) => row.get_string(i).is_err(),
                        Type::Primitive(PrimitiveType::Decimal { .. }) => row.get_decimal(i).is_err(),
                        Type::Primitive(PrimitiveType::Date) => row.get_int(i).is_err(),
                        _ => false,
                    }
                }).count();
                
                if null_count > 0 {
                    println!("WARNING: Field {} has {} null values out of {} rows", 
                             field.name, null_count, rows.len());
                    has_null_required_fields = true;
                }
            }
        }
        
        if has_null_required_fields {
            println!("WARNING: Table {} has null values in required fields. This may cause errors.", table_name);
            
            // For lineitem table, let's try to fix the l_orderkey field
            if table_name == "lineitem" {
                // Print the first few rows to debug
                for (i, row) in rows.iter().take(5).enumerate() {
                    println!("Row {}: {:?}", i, row);
                    
                    // Try different ways to access the orderkey
                    if let Ok(value) = row.get_long(0) {
                        println!("  l_orderkey via get_long(0): {}", value);
                    } else {
                        println!("  l_orderkey via get_long(0): Error");
                    }
                    
                    if let Ok(value) = row.get_int(0) {
                        println!("  l_orderkey via get_int(0): {}", value);
                    } else {
                        println!("  l_orderkey via get_int(0): Error");
                    }
                    
                    if let Ok(value) = row.get_string(0) {
                        println!("  l_orderkey via get_string(0): {}", value);
                    } else {
                        println!("  l_orderkey via get_string(0): Error");
                    }
                }
            }
        }

        // Convert rows to arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (idx, field) in schema.as_struct().fields().iter().enumerate() {
            match field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Int) => {
                    let values: Vec<Option<i32>> = rows.iter().map(|row| {
                        row.get_int(idx).ok()
                    }).collect();
                    arrays.push(Arc::new(Int32Array::from(values)));
                },
                Type::Primitive(PrimitiveType::Long) => {
                    let values: Vec<Option<i64>> = rows.iter().map(|row| {
                        // Try to get the value as an int and convert to long if needed
                        if let Ok(value) = row.get_int(idx) {
                            return Some(value as i64);
                        }
                        row.get_long(idx).ok()
                    }).collect();
                    arrays.push(Arc::new(Int64Array::from(values)));
                },
                Type::Primitive(PrimitiveType::String) => {
                    let values: Vec<Option<String>> = rows.iter().map(|row| {
                        row.get_string(idx).ok().map(|s| s.to_string())
                    }).collect();
                    arrays.push(Arc::new(StringArray::from(values)));
                },
                Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                    // Create a Decimal128Array instead of Float64Array
                    let values: Vec<Option<i128>> = rows.iter().map(|row| {
                        row.get_decimal(idx).ok().map(|d| {
                            let bytes = d.data();
                            let mut padded_bytes = [0u8; 16];
                            let len = bytes.len().min(16);
                            padded_bytes[16 - len..].copy_from_slice(&bytes[..len]);
                            i128::from_be_bytes(padded_bytes)
                        })
                    }).collect();
                    arrays.push(Arc::new(Decimal128Array::from(values)
                        .with_precision_and_scale((*precision).try_into().unwrap(), (*scale).try_into().unwrap())?));
                },
                Type::Primitive(PrimitiveType::Date) => {
                    let values: Vec<Option<i32>> = rows.iter().map(|row| {
                        row.get_int(idx).ok().map(|d| {
                            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                .checked_add_days(chrono::Days::new(d as u64)).unwrap();
                            date.num_days_from_ce() - 719163 // Convert to days since Unix epoch
                        })
                    }).collect();
                    arrays.push(Arc::new(Date32Array::from(values)));
                },
                _ => continue,
            }
        }

        // Create RecordBatch
        let batch = RecordBatch::try_new(SchemaRef::new(arrow_schema), arrays)?;

        // Write batch
        data_writer.write(batch).await?;

        // Close writer and get data files
        let data_files = data_writer.close().await?;

        // Start a new transaction
        let mut transaction = Transaction::new(&table);

        // Add data files to transaction
        let mut fast_append = transaction.fast_append(Some(Uuid::new_v4()), vec![])?;
        fast_append.add_data_files(data_files)?;
        transaction = fast_append.apply().await?;

        // Commit the transaction
        transaction.commit(catalog).await?;

        // Clean up temporary Parquet file
        std::fs::remove_file(temp_parquet)?;
        println!("Completed table: {}", table_name);
    }

    Ok(())
}

async fn create_tpch_schema(table_name: &str) -> AnyhowResult<Schema> {
    let schema_fields = match table_name {
        "lineitem" => vec![
            Arc::new(NestedField::required(1, "l_orderkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "l_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(3, "l_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(4, "l_linenumber", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(5, "l_quantity", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(6, "l_extendedprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(7, "l_discount", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(8, "l_tax", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(9, "l_returnflag", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(10, "l_linestatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(11, "l_shipdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::optional(12, "l_commitdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::optional(13, "l_receiptdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::optional(14, "l_shipinstruct", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(15, "l_shipmode", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(16, "l_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "orders" => vec![
            Arc::new(NestedField::required(1, "o_orderkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "o_custkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(3, "o_orderstatus", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(4, "o_totalprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(5, "o_orderdate", Type::Primitive(PrimitiveType::Date))),
            Arc::new(NestedField::optional(6, "o_orderpriority", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(7, "o_clerk", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(8, "o_shippriority", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(9, "o_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "customer" => vec![
            Arc::new(NestedField::required(1, "c_custkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "c_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "c_address", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(4, "c_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(5, "c_phone", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(6, "c_acctbal", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(7, "c_mktsegment", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(8, "c_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "part" => vec![
            Arc::new(NestedField::required(1, "p_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "p_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "p_mfgr", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(4, "p_brand", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(5, "p_type", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(6, "p_size", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(7, "p_container", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(8, "p_retailprice", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(9, "p_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "partsupp" => vec![
            Arc::new(NestedField::required(1, "ps_partkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "ps_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(3, "ps_availqty", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(4, "ps_supplycost", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(5, "ps_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "supplier" => vec![
            Arc::new(NestedField::required(1, "s_suppkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "s_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "s_address", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(4, "s_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(5, "s_phone", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(6, "s_acctbal", Type::Primitive(PrimitiveType::Decimal { precision: 15, scale: 2 }))),
            Arc::new(NestedField::optional(7, "s_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "nation" => vec![
            Arc::new(NestedField::required(1, "n_nationkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "n_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "n_regionkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(4, "n_comment", Type::Primitive(PrimitiveType::String))),
        ],
        "region" => vec![
            Arc::new(NestedField::required(1, "r_regionkey", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "r_name", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::optional(3, "r_comment", Type::Primitive(PrimitiveType::String))),
        ],
        _ => return Err(anyhow::anyhow!("Unsupported table: {}", table_name)),
    };

    let mut identifier_field_ids = HashSet::new();
    identifier_field_ids.insert(1); // Primary key for all tables is the first field

    Ok(Schema::builder()
        .with_schema_id(0)
        .with_fields(schema_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()?)
} 