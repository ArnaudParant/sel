PREFIX := simple_elastic_language
export BUILD_TAG := $(USER)


docker-%:			projects/%/Dockerfile
	docker build -f projects/$*/Dockerfile --build-arg PROJECT=$* --network host -t "$(PREFIX)_$*:$(BUILD_TAG)" .

docker-test-%:		docker-% projects/%/Dockerfile.test
	docker build -f projects/$*/Dockerfile.test --build-arg BUILD_TAG=$(BUILD_TAG) --network host -t "$(PREFIX)_$*_test:$(BUILD_TAG)" .

lint-%:				docker-test-%
	scripts/pylint.sh "$(PREFIX)_$*_test:$(BUILD_TAG)" $*

tests-%:			docker-test-%
	docker-compose -f projects/$*/tests/docker-compose.yml up -d
	docker-compose -f projects/$*/tests/docker-compose.yml exec tests pytest --cov=$* -vvx tests
	docker-compose -f projects/$*/tests/docker-compose.yml down

upshell-%:			docker-test-%
	$(eval export PROJECT=$*)
	docker-compose -f projects/$*/tests/docker-compose.yml -f docker/docker-compose.add_volumes.yml up -d
	docker-compose -f projects/$*/tests/docker-compose.yml -f docker/docker-compose.add_volumes.yml exec tests bash
	docker-compose -f projects/$*/tests/docker-compose.yml down

start-server:		docker-sel_server
	docker-compose -f projects/sel_server/docker-compose.yml up -d

down-server:
	docker-compose -f projects/sel_server/docker-compose.yml down

install-sphinx:
	sudo pip3 install sphinx sphinx_rtd_theme myst-parser pyPEG2

doc:
	cd projects/sel/docs && make clean html

clean:
	rm -rf projects/*/*/__pycache__ scripts/__pycache__
