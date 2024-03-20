FROM golang:1.22.1-alpine3.19 AS build

WORKDIR /app
COPY . .

RUN go mod download
RUN go mod verify


RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go
# owner public address for this PK is 7e5f4552091a69125d5dfcb7b8c2659029395bdf

FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=build /app/.build/kwild ./kwild

# copy the produced data
COPY --from=build /app/deployments/config.toml /root/.kwild/config.toml

COPY --from=build /app/deployments/config.toml /root/.kwild/config.toml

EXPOSE 50051 50151 8080 26656 26657
ENTRYPOINT ["/app/kwild", "--autogen"]
