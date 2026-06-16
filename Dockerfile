# Multi-stage build. Final image is distroless (no shell, no package
# manager) for minimal attack surface. Roughly ~20MB.
#
# Adopters who need a custom plugin maintain their own Dockerfile that
# COPYs their plugin module into the build stage. Otherwise this image
# is the canonical lib-shipped relay.

FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /out/outbox-relay ./cmd/outbox-relay

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/outbox-relay /usr/local/bin/outbox-relay
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/outbox-relay"]
