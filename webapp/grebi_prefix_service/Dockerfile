FROM rust:1.90.0-bullseye

WORKDIR /app

COPY dataload/grebi_shared ./dataload/grebi_shared
COPY dataload/prefix_maps ./prefix_maps
COPY webapp/grebi_prefix_service ./webapp/grebi_prefix_service

WORKDIR /app/webapp/grebi_prefix_service

RUN cargo build --release && \
    mv target/release/grebi_prefix_service /usr/local/bin/grebi_prefix_service

ENV ROCKET_ADDRESS=0.0.0.0
ENV ROCKET_PORT=8080

EXPOSE 8080

CMD ["grebi_prefix_service"]
