export PROJECT := sel
export BUILD_TAG := $(USER)


docker:			Dockerfile
	docker build -f Dockerfile --network host -t "$(PROJECT):$(BUILD_TAG)" .

docker-test:	docker tests/Dockerfile.test
	docker build -f tests/Dockerfile.test --build-arg BUILD_TAG=$(BUILD_TAG) --network host -t "$(PROJECT)_test:$(BUILD_TAG)" .

lint:			docker-test
	scripts/pylint.sh "$(PROJECT)_test:$(BUILD_TAG)" sel

tests:			docker-test
	docker-compose -f tests/docker-compose.yml up -d
	docker-compose -f tests/docker-compose.yml exec tests pytest --cov=sel -vvx tests
	docker-compose -f tests/docker-compose.yml down

upshell:		docker-test
	docker-compose -f tests/docker-compose.yml -f tests/docker-compose.add_volumes.yml up -d
	docker-compose -f tests/docker-compose.yml -f tests/docker-compose.add_volumes.yml exec tests bash
	docker-compose -f tests/docker-compose.yml down

down-tests:
	docker-compose -f tests/docker-compose.yml down

install-sphinx:
	pip install sphinx sphinx_rtd_theme myst-parser
	pip install -e .

doc:
	cd docs && make clean html

clean:
	rm -rf sel/__pycache__ scripts/__pycache__
