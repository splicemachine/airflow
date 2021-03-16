NAME = splicemachine/airflow
VERSION = 0.0.5

.PHONY: all build run ssh stop clean push realclean

all: build

build:
	docker build -t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest

push:
	docker push $(NAME):${VERSION}
	docker push $(NAME):latest

clean:
	-docker rmi $(NAME):${VERSION}

removenone:
	docker rmi $(docker images | grep '<none>' | tr -s ' ' | cut -d' ' -f 3)
