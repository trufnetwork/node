FROM alpine:3.14

RUN apk add --no-cache curl jq
RUN apk add --no-cache bash

#  Options:
#  --config CONFIG, -c CONFIG
#                         config file(JSON format)
#  --listen-addr LISTEN-ADDR
#                         listen address [default: 0.0.0.0:8090]
#  --log-level LOG-LEVEL, -l LOG-LEVEL
#                         log level, one of debug|info|warn|error|panic|fatal [default: info]
#  --log-outputs LOG-OUTPUTS
#                         log output(space separated), use 'stdout' or 'stderr' for console output, or file path for file output
#  --cors-allow-origins CORS-ALLOW-ORIGINS
#                         cross-origin allow origins(space separated domains, with schema); '*' will allow all origins; NOTE: hot reload won't work on this
#  --tls-cert-file TLS-CERT-FILE
#                         TLS certificate file
#  --tls-key-file TLS-KEY-FILE
#                         TLS key file
#  --cookie-same-site COOKIE-SAME-SITE
#                         cookie SameSite attribute, one of Lax|Strict|None [default: Lax]
#  --backends BACKENDS, -b BACKENDS
#                         Kwild nodes HTTP endpoint list(space separated 'host:port')
#  --schema-sync-interval SCHEMA-SYNC-INTERVAL
#                         database schema sync interval, in seconds [default: 6]
#  --domain DOMAIN, -d DOMAIN
#                         domain name for auth message, schema is required [default: https://yourdoamin.com]
#  --statement STATEMENT, -s STATEMENT
#                         legal statement for auth message [default: I accept the Terms of Service: https://yourdoamin.com/tos]
#  --chain-id CHAIN-ID    chain id for auth message [default: kwil-test-chain]
#  --session-secret SESSION-SECRET
#                         session secret, don't change once it's set
#  --allow-adhoc-query    allow adhoc query. NOTE: hot reload won't work on this [default: false]
#  --allow-deploy-db      allow deploy&drop database. NOTE: hot reload won't work on this [default: false]
#  --ip-request-rate IP-REQUEST-RATE
#                         qps per ip, set 0 to disable rate limit [default: 0]
#  --ip-request-burst IP-REQUEST-BURST
#                         max burst per ip [default: 10]
#  --profilemode          enable profile endpoints [default: false, env: KGW_PROFILE]
#  --devmode              run server in dev mode [default: false, env: KGW_DEV]
#  --help, -h             display this help and exit


WORKDIR /app

RUN curl -L -o pkl https://github.com/apple/pkl/releases/download/0.25.3/pkl-alpine-linux-amd64
RUN chmod +x pkl

# use bash
SHELL ["/bin/bash", "-c"]
CMD ./kgw -c <(./pkl eval kgw-config.pkl) $EXTRA_ARGS
