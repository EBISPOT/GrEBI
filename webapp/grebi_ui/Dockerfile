# syntax=docker/dockerfile:1

# Slim official Node base (Debian bookworm) — replaces the ubuntu:22.04 + the
# NodeSource apt dance. Node is needed at runtime: the entrypoint runs the
# frontend build (npm run build) before serving with Caddy.
FROM node:24-bookworm-slim

# Caddy from the official Cloudsmith repo. --no-install-recommends keeps the
# image lean (avoids pulling build-essential and friends).
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates gnupg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
    && curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > /etc/apt/sources.list.d/caddy-stable.list \
    && apt-get update && apt-get install -y --no-install-recommends caddy \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/grebi_ui

COPY webapp/grebi_ui/package.json webapp/grebi_ui/package-lock.json /opt/grebi_ui/
RUN npm install

COPY docs /opt/grebi_ui/docs
COPY webapp/query2code.mjs webapp/api2code.mjs /opt/grebi_ui/../
COPY webapp/grebi_ui /opt/grebi_ui

# Run as a non-root user; it must own the tree so the runtime build can write dist/.
RUN chmod +x /opt/grebi_ui/entrypoint.dockersh \
    && groupadd -r grebi && useradd -r -g grebi -d /opt/grebi_ui grebi \
    && chown -R grebi:grebi /opt/grebi_ui /opt/query2code.mjs /opt/api2code.mjs

USER grebi
CMD ["/opt/grebi_ui/entrypoint.dockersh"]
