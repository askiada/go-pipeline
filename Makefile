.PHONY: lint
lint: ## Lint it
	docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:v2.0.2 golangci-lint run

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
