###############################################################################
# Stage 1 — Build the Rust binary
###############################################################################
FROM rust:1.90.0-bullseye AS builder

WORKDIR /app

COPY dataload/grebi_shared ./dataload/grebi_shared
COPY dataload/prefix_maps ./prefix_maps
COPY webapp/grebi_prefix_service ./webapp/grebi_prefix_service

WORKDIR /app/webapp/grebi_prefix_service

RUN cargo build --release && \
    mv target/release/grebi_prefix_service /usr/local/bin/grebi_prefix_service

###############################################################################
# Stage 2 — Minimal runtime image (just the binary + data)
###############################################################################
FROM debian:bookworm-slim

WORKDIR /app

# The binary is a self-contained, glibc-linked Rust executable; it needs no
# build toolchain at runtime. Copy only the binary and the prefix maps.
COPY --from=builder /usr/local/bin/grebi_prefix_service /usr/local/bin/grebi_prefix_service
COPY --from=builder /app/prefix_maps ./prefix_maps

# Run as a non-root user. The binary and prefix maps are world-readable, and the
# service only reads its prefix map (read-only), so any uid works.
RUN groupadd -r grebi && useradd -r -g grebi -d /app grebi
USER grebi

ENV ROCKET_ADDRESS=0.0.0.0
ENV ROCKET_PORT=8082

EXPOSE 8082

CMD ["grebi_prefix_service"]
