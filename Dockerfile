FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kv ./cmd/kv

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kv /kv
USER nonroot:nonroot
ENTRYPOINT ["/kv"]
