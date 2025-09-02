pub use arrow;
pub use futures;

use std::fs::Metadata;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use futures::Stream;
use futures::StreamExt;

use futures_util::pin_mut;

use async_stream::try_stream;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;

use arrow::array::{BooleanBuilder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder};

use arrow::record_batch::RecordBatch;

pub struct BasicDirentInfo {
    pub filepath: String,
    pub meta: BasicMetadata,
}

pub struct BasicMetadata {
    pub is_dir: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub len: u64,
    pub permissions: String,
    pub created: Option<SystemTime>,
    pub modified: Option<SystemTime>,
    pub accessed: Option<SystemTime>,
}

pub fn systemtime2unixtime_micros(s: SystemTime) -> Option<u64> {
    s.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|m| m.as_micros() as u64)
}

impl From<Metadata> for BasicMetadata {
    fn from(m: Metadata) -> Self {
        Self {
            is_dir: m.is_dir(),
            is_file: m.is_file(),
            is_symlink: m.is_symlink(),
            len: m.len(),
            permissions: format!("{:#?}", m.permissions()),
            created: m.created().ok(),
            modified: m.modified().ok(),
            accessed: m.accessed().ok(),
        }
    }
}

pub fn basic_dirent_info_schema() -> Schema {
    Schema::new(vec![
        Field::new("filepath", DataType::Utf8, false),
        Field::new("is_dir", DataType::Boolean, false),
        Field::new("is_file", DataType::Boolean, false),
        Field::new("is_symlink", DataType::Boolean, false),
        Field::new("len", DataType::UInt64, false),
        Field::new("permissions", DataType::Utf8, false),
        Field::new(
            "created",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "modified",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "accessed",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ])
}

pub fn path2string<P>(p: P) -> String
where
    P: AsRef<Path>,
{
    p.as_ref().to_string_lossy().to_string()
}

pub async fn path2dinfo<P>(p: P) -> Result<BasicDirentInfo, io::Error>
where
    P: AsRef<Path>,
{
    let m: Metadata = tokio::fs::metadata(p.as_ref()).await?;
    let meta: BasicMetadata = m.into();
    Ok(BasicDirentInfo {
        filepath: path2string(p),
        meta,
    })
}

pub async fn dinfo2batch<S>(dirents: S, schema: SchemaRef) -> Result<RecordBatch, io::Error>
where
    S: Stream<Item = Result<BasicDirentInfo, io::Error>>,
{
    pin_mut!(dirents);

    let mut filepath_builder = StringBuilder::new();
    let mut is_dir_builder = BooleanBuilder::new();
    let mut is_file_builder = BooleanBuilder::new();
    let mut is_symlink_builder = BooleanBuilder::new();
    let mut len_builder = UInt64Builder::new();
    let mut permissions_builder = StringBuilder::new();

    let mut created_builder = TimestampMicrosecondBuilder::new();
    let mut modified_builder = TimestampMicrosecondBuilder::new();
    let mut accessed_builder = TimestampMicrosecondBuilder::new();

    while let Some(rbdi) = dirents.next().await {
        let bdi: BasicDirentInfo = rbdi?;
        let BasicDirentInfo { filepath, meta } = bdi;

        filepath_builder.append_value(&filepath);
        is_dir_builder.append_value(meta.is_dir);
        is_file_builder.append_value(meta.is_file);
        is_symlink_builder.append_value(meta.is_symlink);
        len_builder.append_value(meta.len);
        permissions_builder.append_value(&meta.permissions);

        created_builder.append_option(
            meta.created
                .and_then(systemtime2unixtime_micros)
                .map(|t| t as i64),
        );

        modified_builder.append_option(
            meta.modified
                .and_then(systemtime2unixtime_micros)
                .map(|t| t as i64),
        );

        accessed_builder.append_option(
            meta.accessed
                .and_then(systemtime2unixtime_micros)
                .map(|t| t as i64),
        );
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(filepath_builder.finish()),
        Arc::new(is_dir_builder.finish()),
        Arc::new(is_file_builder.finish()),
        Arc::new(is_symlink_builder.finish()),
        Arc::new(len_builder.finish()),
        Arc::new(permissions_builder.finish()),
        Arc::new(created_builder.finish()),
        Arc::new(modified_builder.finish()),
        Arc::new(accessed_builder.finish()),
    ];

    RecordBatch::try_new(schema, arrays).map_err(io::Error::other)
}

pub async fn filenames2dirents<S>(
    mut names: S,
) -> impl Stream<Item = Result<BasicDirentInfo, io::Error>>
where
    S: Stream<Item = Result<String, io::Error>> + Unpin,
{
    try_stream! {
        while let Some(rstr) = names.next().await {
            let filename: String = rstr?;
            let dinfo: BasicDirentInfo = path2dinfo(filename).await?;
            yield dinfo;
        }
    }
}

/// Creates a record batch from the metadata values of the filenames.
pub async fn filenames2batch<S>(
    names: S,
    schema: Option<SchemaRef>,
) -> Result<RecordBatch, io::Error>
where
    S: Stream<Item = Result<String, io::Error>> + Unpin,
{
    let schema = schema.unwrap_or_else(|| Arc::new(basic_dirent_info_schema()));
    let dirents = filenames2dirents(names).await;
    dinfo2batch(dirents, schema).await
}

/// Gets filenames from stdin.
pub fn stdin2filenames() -> impl Stream<Item = Result<String, io::Error>> {
    let stdin = tokio::io::stdin();
    let br = tokio::io::BufReader::new(stdin);
    let lines = tokio::io::AsyncBufReadExt::lines(br);
    tokio_stream::wrappers::LinesStream::new(lines)
}
