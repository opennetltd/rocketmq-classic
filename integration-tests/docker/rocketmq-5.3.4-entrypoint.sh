#!/bin/bash
set -e

if [[ "$1" == "nameserver" || "${NODE_ROLE:-}" == "nameserver" ]]; then
  shift
  exec ./mqnamesrv "${@}"
elif [[ "$1" == "broker" || "${NODE_ROLE:-}" == "broker" ]]; then
  shift
  exec ./mqbroker "${@}"
else
  exec "$@"
fi
