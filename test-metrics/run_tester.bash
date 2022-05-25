docker build -t test_server .
docker run -it --rm --name test_metrics test_server