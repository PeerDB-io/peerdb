# syntax=docker/dockerfile:1

FROM golang:1.19-bullseye
WORKDIR /root/
COPY flow .
# fetch all dependencies
RUN go mod download
# build the binary from cmd folder
WORKDIR /root/cmd
RUN CGO_ENABLED=0 go build -o /root/peer-flow .
EXPOSE 8112
ENTRYPOINT ["/root/peer-flow", "api", "--port", "8112"]
