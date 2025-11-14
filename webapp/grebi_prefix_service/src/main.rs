#[macro_use] extern crate rocket;

use std::collections::HashMap;
use std::io::BufReader;
use std::path::PathBuf;
use rocket::serde::{Deserialize, Serialize, json::Json};
use rocket::State;
use grebi_shared::prefix_map::{PrefixMap, PrefixMapBuilder};

#[derive(Deserialize)]
struct ReprefixRequest {
    iris_or_curies: Vec<String>,
}

#[derive(Serialize)]
struct ReprefixResponse {
    curies: Vec<String>,
}

struct AppState {
    prefix_map: PrefixMap,
}

#[post("/reprefix", data = "<request>")]
fn reprefix(request: Json<ReprefixRequest>, state: &State<AppState>) -> Json<ReprefixResponse> {
    let results: Vec<String> = request.iris_or_curies
        .iter()
        .map(|s| state.prefix_map.reprefix(s))
        .collect();
    Json(ReprefixResponse { curies: results })
}

#[launch]
fn rocket() -> _ {
    let prefix_map_path = std::env::var("GREBI_PREFIX_MAP_PATH").unwrap();

    let prefix_map = {
        let rdr = BufReader::new(std::fs::File::open(&prefix_map_path).unwrap());
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr)
            .unwrap()
            .into_iter()
            .for_each(|(k, v)| {
                builder.add_mapping(k, v);
            });
        builder.build()
    };

    rocket::build()
        .manage(AppState { prefix_map })
        .mount("/", routes![reprefix])
}
