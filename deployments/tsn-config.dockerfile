# This file is used to generate the configuration file for single node usage
# OR use an externally provided configuration file

# So we may use the same compose file for both development and deployment
# To ensure we can test as close as possible to the production environment

# The objective here is to specify which stage to use by using `target` field
# https://docs.docker.com/compose/compose-file/build/#target


FROM busybox:1.35.0-uclibc as external

# error out if there's already a directory at /root/.kwild
# we don't want to overwrite any existing configuration
RUN test ! -d /root/.kwild

ARG EXTERNAL_CONFIG_PATH=.

COPY $EXTERNAL_CONFIG_PATH /root/.kwild

CMD ["sh", "-c", "echo 'Configuration copied'"]

FROM busybox:1.35.0-uclibc as created

ARG CHAIN_ID=truflation-staging

WORKDIR /app

# error out if there's already a directory at /root/.kwild
# we don't want to overwrite any existing configuration
RUN test ! -d /root/.kwild

COPY  ./.build/kwil-admin /app/kwil-admin

# create entrypoint file
RUN echo "#!/bin/sh" > /app/entrypoint.sh
RUN echo "set -e" >> /app/entrypoint.sh
# should test if the .kwild/private_key is already there, and if not, generate it
RUN echo "if [ ! -f /root/.kwild/private_key ]; \
 then ./kwil-admin setup init --chain-id $CHAIN_ID -o /root/.kwild;\\" \
    >> /app/entrypoint.sh
# else we print a message
RUN echo "else echo 'Configuration already exists'; fi" >> /app/entrypoint.sh

# make the entrypoint file executable
RUN chmod +x /app/entrypoint.sh

CMD ["sh", "-c", "/app/entrypoint.sh"]