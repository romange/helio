#!/bin/sh

set -e

# first arg is `-some-option`
if [ "${1#-}" != "$1" ]; then
    set -- echo_server --logtostderr "$@"
fi

exec "$@"
