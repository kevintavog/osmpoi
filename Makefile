BUILD_ID:=$(shell date +%s)

tojupiter: image push

image:
	gradle shadowJar
	cp build/libs/osmpoi-all.jar docker/osmpoi-all.jar
	docker build ./docker --platform linux/amd64 -t docker.rangic:6000/osmpoi.backend:${BUILD_ID}

push:
	docker push docker.rangic:6000/osmpoi.backend:${BUILD_ID}
