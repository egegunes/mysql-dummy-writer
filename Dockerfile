FROM golang:1.22 AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o mysql-dummy-writer .

FROM alpine:latest

WORKDIR /root/

COPY --from=build /app/mysql-dummy-writer .

CMD ["./mysql-dummy-writer"]

