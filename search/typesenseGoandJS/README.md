INSTALL TYPESENSE VIA DOCKER:
docker pull typesense/typesense:0.22.2

MAKE DIRECTORY:
mkdir /tmp/typesense-data

START UP THE TYPESENSE SERVER:
docker run -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.22.2 \--data-dir /data --api-key=xyz --enable-cors