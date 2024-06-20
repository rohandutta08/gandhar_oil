#!/usr/bin/env bash

echo "starting docker build"
set -e


service_name="integration-transformation-layer"
docker_image_tag="staging-$1"

docker_url=$ECR_REGISTRY


docker build -t $service_name:$docker_image_tag .
docker tag $service_name:$docker_image_tag $docker_url/$service_name:$docker_image_tag
docker push $docker_url/$service_name:$docker_image_tag
echo "script_docker_image =  $service_name:$docker_image_tag"
