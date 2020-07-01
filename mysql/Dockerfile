ARG MYSQL_VERSION=latest
FROM mysql:${MYSQL_VERSION}
RUN apt-get update && apt-get install -y wget
RUN wget -P /docker-entrypoint-initdb.d/ http://ergast.com/downloads/f1db.sql.gz
RUN gunzip /docker-entrypoint-initdb.d/f1db.sql.gz