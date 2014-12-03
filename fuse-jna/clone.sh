#!/bin/sh

#ROOT_PATH=$(cd $(dirname $0) && pwd)
DIRECTORY="$1"

URI="git://github.com/EtiennePerot/fuse-jna"

if [ ! -d "$DIRECTORY" ]; then
  git clone "$URI" "$DIRECTORY"
fi

cd "$DIRECTORY"
git reset --hard "665f03ffa4babc4a99421d303bb7489365475a86"