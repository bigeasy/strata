#!/bin/sh

for file in $(find json -type f); do
  file=$(echo $file | sed 's/^json\///')
  sh convert.sh $file
done
