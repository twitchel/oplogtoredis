#!/bin/bash
set -eu

# CA
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 365 -key ca-key.pem -sha256 -out ca.crt \
  -subj "/C=US/ST=MA/L=Boston/O=Tulip/OU=techops/CN=example.com"

# Redis
openssl genrsa -out redis.key 4096
openssl req -subj "/CN=localhost" -sha256 -new -key redis.key -out server.csr
echo "subjectAltName = DNS:redis" > extfile.cnf
openssl x509 -req -days 365 -sha256 -in server.csr -CA ca.crt -CAkey ca-key.pem \
  -CAcreateserial -out redis.crt -extfile extfile.cnf
cat redis.key redis.crt > rediscert.pem
