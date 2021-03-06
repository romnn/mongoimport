
FROM golang:alpine AS BUILD

LABEL maintainer="contact@romnn.com"

ENV GO111MODULE=on

WORKDIR /app
COPY ./ /app

# This removes debug information from the binary
# Assumes go 1.10+
RUN CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a -ldflags="-w -s" -o app "github.com/romnn/mongoimport/cmd/mongoimport"

FROM gcr.io/distroless/static
COPY --from=BUILD /app/app /app
ENTRYPOINT [ "/app" ]
