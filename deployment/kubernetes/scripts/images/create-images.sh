#!/bin/bash
sudo echo "Creating images..."
cd /mnt/c/code/DistributedSystems

cd ../config-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../key-value-store/common
mvn clean install
echo " "
echo " "


cd ../client
mvn clean install
echo " "
echo " "


cd ../admin-dashboard
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../metadata-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../storage-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


echo "Image creation completed."
echo " "
echo " "

