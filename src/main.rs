#![allow(unused_imports)]
#![allow(warnings)]

use itertools::Itertools;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use deltalake::{
    action::{Action, Add, DeltaOperation, Remove, SaveMode, Stats, OutputMode},
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaTableError, SchemaTypeStruct, StorageError, UriError,
};
use deltalake::{
    arrow::{
        // array::{as_primitive_array, Array},
        // TODO: use for computing column stats
        // compute::kernels::aggregate,
        datatypes::{
            DataType,
            Field,
            Schema as ArrowSchema,
            SchemaRef as ArrowSchemaRef,
            TimeUnit,
            // Int16Type, Int32Type, Int64Type, Int8Type,
            //  UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
        error::ArrowError,
        json::reader::{Decoder, DecoderOptions},
        record_batch::RecordBatch,
    },
    writer::DeltaWriterError,
};
use lazy_static::lazy_static;
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
use regex::Regex;
use serde_json::{json, Value};

struct InMemValueIter<'a> {
    buffer: &'a [Value],
    current_index: usize,
}

impl<'a> InMemValueIter<'a> {
    fn from_vec(v: &'a [Value]) -> Self {
        Self {
            buffer: v,
            current_index: 0,
        }
    }
}

impl<'a> Iterator for InMemValueIter<'a> {
    type Item = Result<Value, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.buffer.get(self.current_index);

        self.current_index += 1;

        item.map(|v| Ok(v.to_owned()))
    }
}

type ArrowDataType = DataType;
fn conv_to_arrow_schema(s: &SchemaTypeStruct) -> Result<ArrowSchema, ArrowError> {
    let fields: Vec<Field> = s
        .get_fields()
        .iter()
        .map(|f| {
            let type_ = match f.get_type() {
                deltalake::SchemaDataType::primitive(p) => {
                    lazy_static! {
                        static ref DECIMAL_REGEX: Regex =
                            Regex::new(r"\((\d{1,2}),(\d{1,2})\)").unwrap();
                    }

                    match p.as_str() {
                        "string" => Ok(ArrowDataType::Utf8),
                        "long" => Ok(ArrowDataType::Int64), // undocumented type
                        "integer" => Ok(ArrowDataType::Int32),
                        "short" => Ok(ArrowDataType::Int16),
                        "byte" => Ok(ArrowDataType::Int8),
                        "float" => Ok(ArrowDataType::Float32),
                        "double" => Ok(ArrowDataType::Float64),
                        "boolean" => Ok(ArrowDataType::Boolean),
                        "binary" => Ok(ArrowDataType::Binary),
                        decimal if DECIMAL_REGEX.is_match(decimal) => {
                            let extract = DECIMAL_REGEX
                                .captures(decimal)
                                .ok_or_else(|| {
                                    ArrowError::SchemaError(format!(
                                        "Invalid decimal type for Arrow: {}",
                                        decimal
                                    ))
                                })
                                .unwrap();
                            let precision = extract
                                .get(1)
                                .and_then(|v| v.as_str().parse::<usize>().ok());
                            let scale = extract
                                .get(2)
                                .and_then(|v| v.as_str().parse::<usize>().ok());
                            match (precision, scale) {
                                (Some(p), Some(s)) => Ok(ArrowDataType::Decimal(p, s)),
                                _ => Err(ArrowError::SchemaError(format!(
                                    "Invalid precision or scale decimal type for Arrow: {}",
                                    decimal
                                ))),
                            }
                        }
                        "date" => {
                            // A calendar date, represented as a year-month-day triple without a
                            // timezone. Stored as 4 bytes integer representing days sinece 1970-01-01
                            Ok(ArrowDataType::Date32)
                        }
                        "timestamp" => {
                            // Issue: https://github.com/delta-io/delta/issues/643
                            Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                        }
                        s => Err(ArrowError::SchemaError(format!(
                            "Invalid data type for Arrow: {}",
                            s
                        ))),
                    }
                }
                deltalake::SchemaDataType::r#struct(_) => todo!(),
                deltalake::SchemaDataType::array(_) => todo!(),
                deltalake::SchemaDataType::map(_) => todo!(),
            };
            Field::new(f.get_name(), type_.unwrap(), f.is_nullable())
        })
        .collect();
    Ok(ArrowSchema::new(fields))
}

pub async fn add_row(table_path: &str, service_area: Value, config: Value) {
    // NOTE: Test table is partitioned by `modified`
    // initialize table and writer
    let mut delta_table = deltalake::open_table(table_path).await.unwrap();

    let metadata = delta_table.get_metadata().unwrap();
    let arrow_schema_ref = Arc::new(conv_to_arrow_schema(&metadata.schema).unwrap());
    let mut writer = RecordBatchWriter::for_table(&delta_table, HashMap::new()).unwrap();

    // let actions = delta_table
    //     .get_files()
    //     .into_iter()
    //     .map(|file| Action::remove(create_remove(file.to_owned())))
    //     .collect::<Vec<Action>>();

    let mut transaction = delta_table.create_transaction(None);

    // let operation = DeltaOperation::Write {
    //     mode: SaveMode::Append,
    //     partition_by: None,
    //     predicate: None,
    // };

    // let operation = DeltaOperation::StreamingUpdate { output_mode: OutputMode::Update, query_id: (), epoch_id: () }

    // transaction.add_actions(actions);
    // transaction.commit(Some(operation), None).await.unwrap();

    let rows = get_all_rows(table_path).await;
    let mut flag = false;

    let mut new_rows = rows
        .into_iter()
        .map(|mut r| {
            if let Some(sa) = r.as_object_mut() {
                match sa.get("id") == Some(&service_area) {
                    true => {
                        sa.remove("config");
                        let new_config = config.to_string();
                        sa.insert(
                            String::from("config"),
                            serde_json::from_str::<Value>(&new_config).unwrap(),
                        );
                        flag = true;
                    }
                    false => {}
                };
            }
            r
        })
        .collect_vec();
    if !flag {
        new_rows.insert(
            0,
            json!({
                "id" : service_area,
                "name" : service_area,
                "config" : config
            }),
        );
    }

    let record_batch =
        record_batch_from_json_buffer(arrow_schema_ref, new_rows.as_slice()).unwrap();
    writer.write(record_batch).await.unwrap();

    let add_actions: Vec<Action> = writer
        .flush()
        .await
        .unwrap()
        .into_iter()
        .map(|add| Action::add(add))
        .collect();

    transaction.add_actions(add_actions);
    transaction.commit(None, None).await.unwrap();
}

fn create_remove(path: String) -> Remove {
    let deletion_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let deletion_timestamp = deletion_timestamp.as_millis() as i64;

    Remove {
        path,
        deletion_timestamp: Some(deletion_timestamp),
        data_change: true,
        extended_file_metadata: Some(false),
        ..Default::default()
    }
}

fn read_file(file_name: &str) -> Vec<Value> {
    let reader = SerializedFileReader::new(std::fs::File::open(file_name).unwrap()).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| row.to_json_value())
        .collect::<Vec<Value>>()
}

async fn get_all_rows(table_path: &str) -> Vec<Value> {
    let table = deltalake::open_table(table_path).await.unwrap();
    let rows = table
        .get_files_iter()
        .flat_map(|file_name| read_file(&format!("{}/{}", table_path, file_name)))
        .collect::<Vec<Value>>();
    rows
}

fn record_batch_from_json_buffer(
    arrow_schema_ref: ArrowSchemaRef,
    json_buffer: &[Value],
) -> Result<RecordBatch, DeltaWriterError> {
    let row_count = json_buffer.len();
    let mut value_ter = InMemValueIter::from_vec(json_buffer);
    let decoder = Decoder::new(
        arrow_schema_ref,
        DecoderOptions::new().with_batch_size(row_count),
    );
    let batch = decoder.next_batch(&mut value_ter).unwrap();

    // handle none
    let batch = batch.unwrap();

    Ok(batch)
}

#[tokio::main]
async fn main() {
    let data = r#"{
        "tuftsmahcmwecw": ["Clinical"],
        "tuftssoarian": ["Clinical"],
        "tuftsecw": ["Clinical"],
        "tuftsmosaiq": ["Clinical"],
        "tuftsedmedhost": ["Clinical"],
        "tuftscentricity": ["Clinical"],
        "tuftstmcccecw": ["Clinical"],
        "tuftsimedicware": ["Clinical"],
        "tuftsmodmed": ["Clinical"],
        "tuftspresidio": ["Clinical"],
        "circlehealthmanwnaecw": ["Clinical"],
        "circlehealthcerner": ["Clinical"],
        "circlehealthmamvimecw": ["Clinical"],
        "circlehealthmamuapecw": ["Clinical"],
        "circlehealthmamcmgecw": ["Clinical"],
        "circlehealthmaluspecw": ["Clinical"],
        "circlehealthmamvcaecw": ["Clinical"],
        "circlehealthvpcpowerworks": ["Clinical"],
        "circlehealthmanrsaecw": ["Clinical"],
        "circlehealthmasainecw": ["Clinical"],
        "circlehealthmanmfmecw": ["Clinical"],
        "circlehealthdawnac": ["Clinical"],
        "melrosecentricity": ["Clinical"],
        "melrosemeditech": ["Clinical"],
        "vpcpowerworkschelmsford": ["Clinical"],
        "homecarehomebase": ["Clinical"],
        "vpcpowerworksdrumhill": []
    }"#;

    let config: Value = serde_json::from_str(data).unwrap();

    // const SERVICE_AREA_PATH: &str =
    //     "/mnt/wiise-etl/datalake/integrationarchive/integrationarchiveglobal/servicearea";
    const SERVICE_AREA_PATH: &str = "/home/bytebaker/.mnt/minio/servicearea_test";

    let service_area = json!("3002");

    add_row(SERVICE_AREA_PATH, service_area, config).await;
}
