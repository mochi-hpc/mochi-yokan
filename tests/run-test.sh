#!/bin/bash

CMD=$1

if [[ -z "${YOKAN_TEST_BACKEND}" ]]; then
  $CMD
else
  $CMD --param backend ${YOKAN_TEST_BACKEND}
fi
