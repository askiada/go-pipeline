.PHONY: lint
lint: ## Lint it
	golangci-lint run --verbose ./...

.PHONY: test
test:
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
