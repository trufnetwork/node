FROM golang:1.22.7-alpine3.19 AS build

WORKDIR /app
COPY go.mod go.sum ./

RUN go mod download
RUN go mod verify

COPY ./scripts/download-binaries.sh ./scripts/download-binaries.sh
RUN chmod +x ./scripts/download-binaries.sh
# download the kwil binaries
RUN sh ./scripts/download-binaries.sh

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go
# owner public address for this PK is 7e5f4552091a69125d5dfcb7b8c2659029395bdf

FROM busybox:1.35.0-uclibc as busybox
# busy box will provide us with a shell to run commands in distroless

FROM alpine:latest

WORKDIR /app

# add postgres client tools
RUN apk add --no-cache postgresql16-client

# move .build content to /app
COPY --from=build /app/.build/* /app/

# move the entrypoint script to /app
COPY --from=build /app/deployments/tsn-entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

EXPOSE 50051 50151 8080 8484 26656 26657
ENTRYPOINT /app/entrypoint.sh
