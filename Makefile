IMAGE_BASE=egegunes/mysql-dummy-writer
VERSION:=$(shell date +%s)
IMAGE=$(IMAGE_BASE):$(VERSION)

build:
	docker buildx build --push -t $(IMAGE) .
deploy:
	yq eval \
		'.spec.template.spec.containers[0].image = "$(IMAGE)"' deployment.yaml \
		| kubectl apply -f -
