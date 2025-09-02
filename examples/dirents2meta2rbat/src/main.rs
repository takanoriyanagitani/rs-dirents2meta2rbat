use std::io;
use std::process::ExitCode;

use rs_dirents2meta2rbat::arrow;

use arrow::record_batch::RecordBatch;

use rs_dirents2meta2rbat::filenames2batch;
use rs_dirents2meta2rbat::stdin2filenames;

fn print_batch(b: &RecordBatch) -> Result<(), io::Error> {
    println!("{b:#?}");
    Ok(())
}

async fn sub() -> Result<(), io::Error> {
    let filenames = stdin2filenames();
    let b: RecordBatch = filenames2batch(filenames, None).await?;
    print_batch(&b)
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
