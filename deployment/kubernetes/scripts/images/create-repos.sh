#!/bin/bash
aws ecr create-repository --repository-name config-server --region us-east-1
aws ecr create-repository --repository-name kvs-admin-dashboard --region us-east-1
aws ecr create-repository --repository-name kvs-storage-server --region us-east-1
aws ecr create-repository --repository-name kvs-metadata-server --region us-east-1