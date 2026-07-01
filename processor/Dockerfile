# Build a static binary, then ship it on a tiny base (mirrors the simulator image).
FROM golang:1.26 AS build
WORKDIR /src
# go.sum is committed, so the build is hermetic: `go mod download` verifies every
# dependency against the checksum file with no resolution step. Copying the module
# manifests first keeps that layer cached across source-only changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/processor .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/processor /app/processor
EXPOSE 9102
ENTRYPOINT ["/app/processor"]
