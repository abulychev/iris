#!/bin/sh

BIN_PATH=$(cd $(dirname $0) && pwd)
ROOT_PATH=${BIN_PATH}/..

DATA_PATH=$1
MOUNT_POINT=$2

exec \
java -cp "$ROOT_PATH/lib/*" \
     -showversion -server \
     -Dfile.encoding=UTF8 \
     -Xmx1g \
     -Djava.net.preferIPv4Stack=true \
     com.github.abulychev.iris.application.Main ${DATA_PATH} ${MOUNT_POINT}