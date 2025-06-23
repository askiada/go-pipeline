.PHONY: lint
lint: ## Lint it
	docker run --rm -t -v $$(pwd):/app -w /app \
	--user $$(id -u):$$(id -g) \
	-v $$(go env GOCACHE):/.cache/go-build -e GOCACHE=/.cache/go-build \
	-v $$(go env GOMODCACHE):/.cache/mod -e GOMODCACHE=/.cache/mod \
	-v ~/.cache/golangci-lint:/.cache/golangci-lint -e GOLANGCI_LINT_CACHE=/.cache/golangci-lint \
	golangci/golangci-lint:v2.1.6 golangci-lint run

.PHONY: unit_test
unit_test:
	go test -race -timeout 30s ./...
	dot -Tpng -O pkg/pipeline/mygraph.gv
	dot -Tpng -O pkg/pipeline/mygraph-simple.gv
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter.gv
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v2.gv
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v3.gv
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v4.gv

	rm pkg/pipeline/mygraph.gv
	rm pkg/pipeline/mygraph-simple.gv
	rm pkg/pipeline/mygraph-simple-splitter.gv
	rm pkg/pipeline/mygraph-simple-splitter-v2.gv
	rm pkg/pipeline/mygraph-simple-splitter-v3.gv
	rm pkg/pipeline/mygraph-simple-splitter-v4.gv
