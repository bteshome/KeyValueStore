#!/bin/bash

echo "Tagging images..."
docker tag bteshome/config-server:latest 111111111111.dkr.ecr.us-east-1.amazonaws.com/config-server:latest
docker tag bteshome/kvs-admin-dashboard:latest 111111111111.dkr.ecr.us-east-1.amazonaws.com/kvs-admin-dashboard:latest
docker tag bteshome/kvs-storage-server:latest 111111111111.dkr.ecr.us-east-1.amazonaws.com/kvs-storage-server:latest
docker tag bteshome/kvs-metadata-server:latest 111111111111.dkr.ecr.us-east-1.amazonaws.com/kvs-metadata-server:latest
echo "Image tagging completed successfully!"
echo " "
echo " "

