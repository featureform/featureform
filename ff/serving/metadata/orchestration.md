# Simple dashboard w/o metrics

## Features: metadata (M), search (S), gui (G)

requires = requires complete system to initialize
runsw = interacts with running endpoint during deployment
-> _ = fully deploys _ letter of orchestration upon start
^ = runs once and terminates
1(runsw 2 3 4) Start metadata server (./main go run server.go)
2(runsw 1 5) Start typesense server (bash ./startTypeSense.bash) -> M -> S
^(requires MS runsw 1 2) Upload sample data (./main go run client.go)
4(runsw 1 5) Start metadata dashboard server (./dashboard go run dashboard_metadata.go)
5(runsw 2 4) Start dashboard (../../../../dashboard npm start) -> G

# Simple dashboard w/ metrics

## Features: metadata (Meta), Serving Stub (Serv), Prometheus (Prom), search (Search), gui (Gui)

requires = requires complete system to initialize
runsw = interacts with running endpoint during deployment
-> _ = fully deploys _ letter of orchestration upon start
^ = runs once and terminates
1(runsw 2 3 4) Start metadata server (./main go run server.go)
2(runsw 1 5) Start typesense server (bash ./startTypeSense.bash) -> M -> S
^(requires MS runsw 1 2) Upload sample data (./main go run client.go)
4(runsw 1 5) Start metadata dashboard server (./dashboard go run dashboard_metadata.go)
5(runsw 2 4) Start dashboard (../../../../dashboard npm start) -> G
