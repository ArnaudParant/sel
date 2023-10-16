#!/bin/bash

docker run --entrypoint=pylint $1 $2 -f parseable
RETVAL=$?

# Pylint return codes that are OK
BADCODES=("1" "2" "32")

for BADCODE in "${BADCODES[@]}"
do
    if [ $(($RETVAL & $BADCODE)) != 0 ] ; then
        echo "error: pylint exited with code $RETVAL"
        exit 1
    fi
done

echo "pylint exited with code $RETVAL"
