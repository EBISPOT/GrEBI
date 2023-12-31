

use clap::Parser;
use rocksdb::DB;
use rocksdb::IteratorMode;
use rocksdb::Options;

mod slice_entity;
use slice_entity::SlicedEntity;


#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    rocksdb_path: String,

    #[arg(short, long)]
    output_filename: String
}

fn main() -> std::io::Result<()> {

    let args = Args::parse();

    let start_time = std::time::Instant::now();

    let mut options = Options::default();
    let db = DB::open(&options, args.rocksdb_path).unwrap();

    let mut iter = db.iterator(IteratorMode::Start);

    let mut not_exist = 0;
    let mut yes_exist = 0;

    let mut n:i64 = 0;

    for res in iter {

        let res_u = res.unwrap();
        let value = res_u.1;
        let value_vec = value.into_vec();

        let sliced = SlicedEntity::from_json(&value_vec);

        sliced.props.iter().for_each(|prop| {

            n = n + 1;
            if n % 1000000 == 0 {
                println!("{}", n);
            }

            if prop.value.contains(&b':') {

                let exists = db.get(prop.value).is_ok();

                if exists {
                    yes_exist = yes_exist + 1;
                } else {
                    not_exist = not_exist + 1;
                }
            }

        });
    }

    eprintln!("Traversing and looking up all prop values took {} seconds", start_time.elapsed().as_secs());

    println!("yes_exist {} not_exist {}", yes_exist, not_exist);

    Ok(())
}
