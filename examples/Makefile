EXAMPLE_DIRS := $(patsubst %/,%,$(sort $(dir $(wildcard */main.go))))
DOT ?= dot
DRAWER_FLAG ?= -drawer

.PHONY: all list run $(EXAMPLE_DIRS)

all: $(EXAMPLE_DIRS)

list:
	@printf "%s\n" $(EXAMPLE_DIRS)

run:
	@if [ -z "$(EXAMPLE)" ]; then \
		echo "Usage: make run EXAMPLE=<name>"; \
		exit 1; \
	fi
	@if [ ! -d "$(EXAMPLE)" ]; then \
		echo "Unknown example: $(EXAMPLE)"; \
		exit 1; \
	fi
	go run ./$(EXAMPLE) $(DRAWER_FLAG)
	$(DOT) -Tpng $(EXAMPLE)/pipeline.dot -o $(EXAMPLE)/pipeline.png

$(EXAMPLE_DIRS):
	go run ./$@ $(DRAWER_FLAG)
	$(DOT) -Tpng $@/pipeline.dot -o $@/pipeline.png
