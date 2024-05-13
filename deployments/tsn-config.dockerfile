# This file is used to generate the configuration file for single node usage
# OR use an externally provided configuration file

# So we may use the same compose file for both development and deployment
# To ensure we can test as close as possible to the production environment

# The objective here is to specify which stage to use by using `target` field
# https://docs.docker.com/compose/compose-file/build/#target


FROM busybox:1.35.0-uclibc as external

ARG EXTERNAL_CONFIG_PATH
ONBUILD COPY ${EXTERNAL_CONFIG_PATH} /root/.kwild

FROM busybox:1.35.0-uclibc as created

ARG CHAIN_ID=truflation-staging
ARG DB_HOST=kwil-postgres

WORKDIR /app

ONBUILD COPY  ./.build/kwil-admin /app/kwil-admin

ONBUILD RUN ./kwil-admin setup init --chain-id $CHAIN_ID -o /root/.kwild