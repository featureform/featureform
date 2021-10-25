docker build -t local-eh -f Dockerfile.update .
docker run --name local-eh --rm -it -p 7462:7462 local-eh