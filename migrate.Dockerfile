FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" ./cmd/migrate

FROM alpine:3.20
COPY --from=build /src/migrate /migrate
ENTRYPOINT ["/migrate"]
