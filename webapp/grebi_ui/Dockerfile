# syntax=docker/dockerfile:1

# Red Hat UBI9-minimal + Node 24 (NodeSource EL9) + Caddy (static binary).
# Node is needed at runtime: the entrypoint runs the frontend build
# (npm run build) before serving with Caddy. bash is needed by entrypoint.dockersh.
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

RUN printf '%s\n' \
      '[nodesource-nodejs]' \
      'name=Node.js 24.x' \
      'baseurl=https://rpm.nodesource.com/pub_24.x/nodistro/nodejs/$basearch' \
      'enabled=1' 'gpgcheck=0' > /etc/yum.repos.d/nodesource.repo && \
    microdnf install -y --setopt=install_weak_deps=0 bash nodejs shadow-utils tar gzip && \
    microdnf clean all

# Caddy upstream static binary (arch-aware).
RUN arch="$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')" && \
    curl -L "https://caddyserver.com/api/download?os=linux&arch=${arch}" -o /usr/bin/caddy && \
    chmod +x /usr/bin/caddy

WORKDIR /opt/grebi_ui

COPY webapp/grebi_ui/package.json webapp/grebi_ui/package-lock.json /opt/grebi_ui/
RUN npm install

COPY docs /opt/grebi_ui/docs
COPY webapp/query2code.mjs webapp/api2code.mjs /opt/grebi_ui/../
COPY webapp/grebi_ui /opt/grebi_ui

# Run as a non-root user; it must own the tree so the runtime build can write dist/.
RUN chmod +x /opt/grebi_ui/entrypoint.dockersh && \
    groupadd -r grebi && useradd -r -g grebi -d /opt/grebi_ui grebi && \
    chown -R grebi:grebi /opt/grebi_ui /opt/query2code.mjs /opt/api2code.mjs

USER grebi
CMD ["/opt/grebi_ui/entrypoint.dockersh"]
