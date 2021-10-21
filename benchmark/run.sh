#!/bin/bash

rm -f benchmark-config.json
cat > benchmark-config.json <<- EOM
{
    "libraries": {
        "yokan": "src/libyokan-bedrock-module.so"
    },
    "providers": [
        {
            "name" : "yokan_provider",
            "type" : "yokan",
            "config" : {
                "databases": [
                    {
                        "type": "map"
                    }
                ]
            }
        }
    ]
}
EOM

echo "Starting Bedrock..."

bedrock na+sm -c benchmark-config.json > bedrock.log 2>&1 &
BEDROCK_PID=$!

sleep 1

echo "Extracting database id and server address..."

DATABASE_ID=`awk 'FNR == 1 {print $7}' bedrock.log`
SVR_ADDRESS=`awk 'FNR == 3 {print $9}' bedrock.log`

echo "Database id is ${DATABASE_ID}"
echo "Server address is ${SVR_ADDRESS}"

echo "Starting benchmark"

function run_benchmark {
    echo "=================================="
    echo "Running \"$1\" benchmark"
    benchmark/yokan-benchmark           \
        --operation $1                  \
        --key-sizes 16,32               \
        --value-sizes 128,256           \
        --num-items 16384               \
        --repetitions 8                 \
        --database-id ${DATABASE_ID}    \
        --server-address ${SVR_ADDRESS} \
        --batch-size 32
}

run_benchmark put
run_benchmark put_multi
run_benchmark put_packed
run_benchmark get
run_benchmark get_multi
run_benchmark get_packed
run_benchmark length
run_benchmark length_multi
run_benchmark length_packed
run_benchmark exists
run_benchmark exists_multi
run_benchmark exists_packed
run_benchmark erase
run_benchmark erase_multi
run_benchmark erase_packed
run_benchmark list_keys
run_benchmark list_keys_packed
run_benchmark list_keyvals
run_benchmark list_keyvals_packed

echo "=================================="
echo "Killing Bedrock"

kill $BEDROCK_PID
