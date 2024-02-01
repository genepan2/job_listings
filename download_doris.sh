#!/bin/bash

# Version variable
VERSION="2.0.3"
TEMPDIR="_temp"

# Download the Apache Doris binary
wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-$VERSION-bin-x64.tar.gz
mkdir -p ./$TEMPDIR

# Untar the downloaded file
tar -zxvf apache-doris-$VERSION-bin-x64.tar.gz -C ./$TEMPDIR

# Navigate to the untarred directory
cd apache-doris-$VERSION-bin-x64

# Tar the be, fe, and broker directories
tar -zcvf apache-doris-$VERSION-bin-be-x64.tar.gz be/
tar -zcvf apache-doris-$VERSION-bin-fe-x64.tar.gz fe/
tar -zcvf apache-doris-$VERSION-bin-broker-x64.tar.gz extensions/apache_hdfs_broker/

# Move the tarred files to their respective resource directories
mkdir -p ../doris/be/resource
mkdir -p ../doris/fe/resource
mkdir -p ../doris/broker/resource

mv apache-doris-$VERSION-bin-be-x64.tar.gz ../doris/be/resource/
mv apache-doris-$VERSION-bin-fe-x64.tar.gz ../doris/fe/resource/
mv apache-doris-$VERSION-bin-broker-x64.tar.gz ../doris/broker/resource/

# Navigate back to the parent directory
cd ..

# Delete the downloaded file and the untarred folder
rm -rf apache-doris-$VERSION-bin-x64.tar.gz apache-doris-$VERSION