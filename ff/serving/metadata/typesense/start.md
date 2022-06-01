INSTALL TYPESENSE VIA DOCKER:
docker pull typesense/typesense:0.22.2

MAKE DIRECTORY:
mkdir /tmp/typesense-data

START UP THE TYPESENSE SERVER:
docker run -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.22.2 --data-dir /data --api-key=xyz --enable-cors

install javascript client library:
npm install --save typesense
npm install --save @babel/runtime

install go typesense client library:
go get github.com/typesense/typesense-go

index files:
go run index.go
This conversation was marked as resolved by saadhvi27
 Show conversation

This conversation was marked as resolved by saadhvi27
 Show conversation
run search:
node test.js