FROM docker.io/golang:1.20 as builder
WORKDIR /root/brul2influx
COPY vendor vendor
COPY brul2influx.go go.mod go.sum /root/brul2influx/
RUN GOOS=linux go build -o brul2influx brul2influx.go

FROM debian:12
WORKDIR /root/
COPY --from=builder /root/brul2influx/brul2influx .
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
CMD ["/root/brul2influx"]
