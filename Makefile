.PHONY: lint
lint: ## Lint it
	golangci-lint run --verbose ./...

.PHONY: unit_test
unit_test:
	go test -race -timeout 30s ./...
	dot -Tpng -O pkg/pipeline/mygraph.dot
	dot -Tpng -O pkg/pipeline/mygraph-simple.dot
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter.dot
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v2.dot
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v3.dot
	dot -Tpng -O pkg/pipeline/mygraph-simple-splitter-v4.dot

	rm pkg/pipeline/mygraph.dot
	rm pkg/pipeline/mygraph-simple.dot
	rm pkg/pipeline/mygraph-simple-splitter.dot
	rm pkg/pipeline/mygraph-simple-splitter-v2.dot
	rm pkg/pipeline/mygraph-simple-splitter-v3.dot
	rm pkg/pipeline/mygraph-simple-splitter-v4.dot

.PHONY: example_metrics_drawer
example_metrics_drawer:
	go run ./examples/metrics-drawer
	dot -Tpng examples/metrics-drawer/pipeline.dot -o examples/metrics-drawer/pipeline.png
