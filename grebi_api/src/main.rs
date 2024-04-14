
#[macro_use] extern crate rocket;

#[get("/test/<id>")]
fn hello(id: &str) -> String {
    format!("id was {}", id)
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![hello])
}


