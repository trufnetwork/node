FROM alpine:3.14

RUN apk add --no-cache curl jq

WORKDIR /app

# we expect the user to provide the binary path available at the build context
ARG SESSION_SECRET
ARG CORS_ALLOWED_ORIGINS
ARG DOMAIN

COPY ./kgw.base-config.json ./config/config.json

# the command needed is
# /app/kgw --config ./config/config.json --session-secret $SESSION_SECRET \
#    --cors-allow-origins $CORS_ALLOWED_ORIGINS --domain $DOMAIN"

ENV SESSION_SECRET=$SESSION_SECRET
ENV CORS_ALLOWED_ORIGINS=$CORS_ALLOWED_ORIGINS
ENV DOMAIN=$DOMAIN

# todo: parameterize tsn-db port
COPY ./entrypoint.sh ./entrypoint.sh

# make the entrypoint executable
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

CMD /app/kgw --config ./config/config.json --session-secret $SESSION_SECRET \
     --domain $DOMAIN $CORS_ARGS --chain-id $CHAIN_ID