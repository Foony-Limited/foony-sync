# Builds the foony-sync agent into a single static binary on a scratch base:
# no shell and no OS packages, so there is nothing to patch or exploit in the
# image beyond the agent itself.
FROM golang:1.26.3 AS builder

WORKDIR /src
COPY . .
RUN go mod download

ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -tags netgo,osusergo \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/foony-sync .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/foony-sync /foony-sync

ENTRYPOINT ["/foony-sync"]
