To create a new cert:
In cert.conf, change DNS.1 to desire domain name
From charts directory, run:
`
openssl genrsa -out cert/server.key 2048
openssl req -nodes -new -x509 -sha256 -days 1825 -config cert/cert.conf -extensions 'req_ext' -key cert/server.key -out cert/server.crt
`

`
CERT_NAME=tls-secret
KEY_FILE=cert/server.key
CERT_FILE=cert/server.crt
kubectl create secret tls ${CERT_NAME} --key ${KEY_FILE} --cert ${CERT_FILE}
`