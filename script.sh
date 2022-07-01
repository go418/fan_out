go test -coverprofile=cover.cov ./...
go tool cover -html=cover.cov -o cover.html
python3 -m http.server

go test -cpuprofile cpu.prof -memprofile mem.prof -bench .
go test -cpuprofile cpu.prof -memprofile mem.prof -mutexprofile mut.prof -blockprofile block.prof -bench .
go tool pprof -http :8080 mem.prof