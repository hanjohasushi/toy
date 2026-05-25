#!/bin/bash

while true
do
    inotifywait -r -e modify,create,delete,move .

    git add .

    git commit -m "Auto commit $(date '+%Y-%m-%d %H:%M:%S')" || true

    git push
done
