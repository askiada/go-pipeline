.PHONY: lint
lint: ## Lint it
	golangci-lint run --verbose ./...

DOT_DIR := $(CURDIR)/pkg/pipeline

.PHONY: unit_test
unit_test:
	PIPELINE_DOT_OUTPUT_DIR=$(DOT_DIR) go test -race -timeout 30s ./...
	dot -Tpng -O $(DOT_DIR)/mygraph.dot
	dot -Tpng -O $(DOT_DIR)/mygraph-simple.dot
	dot -Tpng -O $(DOT_DIR)/mygraph-simple-splitter.dot
	dot -Tpng -O $(DOT_DIR)/mygraph-simple-splitter-v2.dot
	dot -Tpng -O $(DOT_DIR)/mygraph-simple-splitter-v3.dot
	dot -Tpng -O $(DOT_DIR)/mygraph-simple-splitter-v4.dot

	rm $(DOT_DIR)/mygraph.dot
	rm $(DOT_DIR)/mygraph-simple.dot
	rm $(DOT_DIR)/mygraph-simple-splitter.dot
	rm $(DOT_DIR)/mygraph-simple-splitter-v2.dot
	rm $(DOT_DIR)/mygraph-simple-splitter-v3.dot
	rm $(DOT_DIR)/mygraph-simple-splitter-v4.dot

EXAMPLE_DIRS := $(patsubst %/,%,$(sort $(dir $(wildcard examples/*/main.go))))

.PHONY: examples_all
examples_all:
	$(MAKE) -C examples all

.PHONY: examples_run
examples_run:
	$(MAKE) -C examples run EXAMPLE=$(EXAMPLE)

.PHONY: examples_list
examples_list:
	$(MAKE) -C examples list

.PHONY: $(addprefix examples_,$(EXAMPLE_DIRS))
$(addprefix examples_,$(EXAMPLE_DIRS)):
	$(MAKE) -C examples $(@:examples_%=%)
