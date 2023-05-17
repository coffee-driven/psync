build:
	@echo "### Building"
	python3 -m build

install: build
	@echo "========================================="
	@echo "### Installing locally from build directory"
	pip3 install dist/psync-*.tar.gz
