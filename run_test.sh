#!/bin/bash

crystal build -p -s -t --error-trace ./examples/network_test.cr

# Configuration
NODE_COUNT=50    # Total nodes (including bootstrap)
MESSAGE_COUNT=10  # Number of test messages
WAIT_TIME=10     # Seconds to wait for ACKs
BASE_PORT=7001   # Starting port

# Clean up processes on exit
declare -a PIDS
function cleanup() {
    echo "Cleaning up processes..."
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null
    done
    exit
}
trap cleanup INT TERM EXIT

# Ensure the network_test.cr file is compiled
if [ ! -f network_test ]; then
    echo "Compiling network_test.cr..."
    crystal build network_test.cr
fi

# Start bootstrap node
echo "Starting bootstrap node..."
BOOTSTRAP_PORT=$BASE_PORT
BOOTSTRAP_ID="node1@localhost:$BOOTSTRAP_PORT"
./network_test --role=bootstrap --port=$BOOTSTRAP_PORT --id=$BOOTSTRAP_ID &
PIDS+=($!)

# Wait for bootstrap node to start
echo "Waiting for bootstrap node to initialize..."
sleep 1

# Start target nodes
echo "Starting $((NODE_COUNT-2)) target nodes..."
for i in $(seq 2 $((NODE_COUNT-1))); do
    PORT=$((BASE_PORT+i-1))
    NODE_ID="node${i}@localhost:$PORT"
    ./network_test --role=target --port=$PORT --id=$NODE_ID --bootstrap=$BOOTSTRAP_ID &
    PIDS+=($!)
    sleep 0.5
done

# Allow time for network to stabilize
echo "Waiting for network to stabilize..."
sleep 5

# Start test node
echo "Starting test node..."
TEST_PORT=$((BASE_PORT+NODE_COUNT-1))
TEST_ID="test@localhost:$TEST_PORT"
./network_test --role=test --port=$TEST_PORT --id=$TEST_ID --bootstrap=$BOOTSTRAP_ID --nodes=$NODE_COUNT --messages=$MESSAGE_COUNT --wait=$WAIT_TIME

# Get test result
TEST_RESULT=$?

# Report result and exit with appropriate code
if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ Test completed successfully"
    exit 0
else
    echo "❌ Test failed with exit code $TEST_RESULT"
    exit 1
fi
