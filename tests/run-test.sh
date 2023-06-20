#!/bin/bash

CMD=$1

if [[ -z "${YOKAN_TEST_BACKEND}" ]]; then
  $CMD --color always
else
  $CMD --color always --param backend ${YOKAN_TEST_BACKEND}
fi
