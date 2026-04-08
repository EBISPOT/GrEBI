
FROM ubuntu:22.04

RUN apt update && apt install -y curl gpg

# node
RUN curl -sL https://deb.nodesource.com/setup_18.x | bash

# caddy
RUN curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
RUN curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list


RUN apt update && apt install -y nodejs caddy

RUN mkdir /opt/grebi_ui


WORKDIR /opt/grebi_ui


COPY webapp/grebi_ui/package.json webapp/grebi_ui/package-lock.json /opt/grebi_ui/
RUN npm install

COPY docs /opt/grebi_ui/docs
COPY webapp/query2code.mjs webapp/api2code.mjs /opt/grebi_ui/../
COPY webapp/grebi_ui /opt/grebi_ui

RUN chmod +x /opt/grebi_ui/entrypoint.dockersh

CMD ["/opt/grebi_ui/entrypoint.dockersh"]

