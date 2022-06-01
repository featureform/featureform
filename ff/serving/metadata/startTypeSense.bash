docker pull typesense/typesense:0.22.2
mkdir /tmp/typesense-data
docker run -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.22.2 --data-dir /data --api-key=xyz --enable-cors
