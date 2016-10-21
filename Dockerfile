FROM golang:1.7.1-wheezy

RUN apt-get update \
  && apt-get install -y \
    mysql-server \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/meta-closure/migo
COPY . .

CMD go test -v .
