FROM golang:1.17 as builder
WORKDIR /root/brul2influx
COPY lib lib
COPY brul2influx.go go.mod go.sum .
RUN GOOS=linux go build -o brul2influx brul2influx.go

FROM debian:11
WORKDIR /root/
COPY --from=builder /root/brul2influx/brul2influx .
CMD ["/root/brul2influx"]
