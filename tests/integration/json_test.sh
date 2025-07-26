#!/bin/bash
# ==============================================================================
# Simplified Test Script for SpinelDB JSON Features
#
# This script is designed to be run while the SpinelDB server is ALREADY
# running in a separate terminal. It does NOT start or stop the server.
#
# Its sole purpose is to:
# 1. Execute a series of JSON command tests using redis-cli.
# 2. Compare the actual output with the expected output.
# 3. Report success or failure for each test case.
#
# Usage:
#   1. Start the SpinelDB server in one terminal: `cargo run`
#   2. In another terminal, make this script executable: `chmod +x run_tests_only.sh`
#   3. Run the script: `./json_test.sh`
# ==============================================================================

# --- Configuration ---
HOST="127.0.0.1"
PORT="7878"

# --- Colors for Output ---
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --- Helper Function ---

# Executes a command and checks its output against an expected value.
# This version is robust for comparing errors, nil, and integer formats.
# Usage: assert_command "Description of test" "redis-cli command" "Expected output"
assert_command() {
    local description="$1"
    local command="$2"
    local expected="$3"

    # Pad the description for aligned output
    printf "Test: %-50s... " "$description"

    # Execute the command. Redirect stderr to stdout (2>&1) to capture everything.
    actual=$(eval "redis-cli -h $HOST -p $PORT $command" 2>&1)

    local pass=0
    # Logic for flexible checking
    if [[ "$expected" == "(nil)" ]]; then
        # For (nil), accept either '(nil)' or an empty string.
        if [[ "$actual" == "(nil)" || -z "$actual" ]]; then
            pass=1
        fi
    elif [[ "$expected" == "(integer) "* ]]; then
        # For integers, check against the raw number or the `(integer) number` format.
        local expected_num="${expected//(integer) /}"
        if [[ "$actual" == "$expected" || "$actual" == "$expected_num" ]]; then
            pass=1
        fi
    elif [[ "$actual" == "$expected" ]]; then
        # For exact string matches (like OK, or JSON arrays)
        pass=1
    elif [[ "$actual" == *"$expected"* && ( "$actual" == *"ERR"* || "$actual" == *"path does not exist"* || "$actual" == *"key or path does not exist"* ) ]]; then
        # For error messages, 'contains' is sufficient.
        pass=1
    fi

    if [[ $pass -eq 1 ]]; then
        echo -e "${GREEN}PASS${NC}"
    else
        echo -e "${RED}FAIL${NC}"
        echo -e "  - Command:  ${YELLOW}$command${NC}"
        # Display clearer expected message
        if [[ "$expected" == "(nil)" ]]; then
            echo -e "  - Expected to be:    ${GREEN}'(nil)' or an empty string${NC}"
        elif [[ "$expected" == "(integer) "* ]]; then
            local expected_num="${expected//(integer) /}"
            echo -e "  - Expected to be:    ${GREEN}'$expected' or '$expected_num'${NC}"
        else
            echo -e "  - Expected:          ${GREEN}'$expected'${NC}"
        fi
        echo -e "  - Got:                 ${RED}'$actual'${NC}"
        # Exit on first failure to make debugging easier
        exit 1
    fi
}

# --- Main Execution ---

echo -e "${YELLOW}--- Checking Server Connection ---${NC}"

# Check if the server is responsive before starting tests
if ! redis-cli -h $HOST -p $PORT PING > /dev/null 2>&1; then
    echo -e "${RED}Error: Could not connect to SpinelDB server on $HOST:$PORT.${NC}"
    echo "Please ensure the server is running in a separate terminal before executing this script."
    exit 1
fi
echo -e "${GREEN}Connection successful.${NC}"


echo -e "\n${YELLOW}--- Running JSON Tests ---${NC}"

# Setup: Clear DB and set initial JSON data
assert_command "Setup: FLUSHDB" "FLUSHDB" "OK"
assert_command "Setup: JSON.SET initial data" "JSON.SET user:101 . '{ \"name\": \"john\", \"logins\": 9, \"balance\": 150.75, \"skills\": [\"rust\", \"python\"] }'" "OK"

# Test 1: JSON.NUMINCRBY and smart number formatting in JSON.GET
assert_command "NUMINCRBY: Increment integer" "JSON.NUMINCRBY user:101 .logins 1" "10"
assert_command "GET: Verify integer format" "JSON.GET user:101 .logins" "10"
assert_command "NUMINCRBY: Increment float to integer" "JSON.NUMINCRBY user:101 .balance 0.25" "151"
assert_command "GET: Verify float-to-integer format" "JSON.GET user:101 .balance" "151"
assert_command "SET: Set float with decimal" "JSON.SET user:101 .balance 151.5" "OK"
assert_command "GET: Verify float with decimal format" "JSON.GET user:101 .balance" "151.5"

# Test 2: Verify `create_if_not_exist: false` logic
assert_command "NUMINCRBY: Fail on non-existent path" "JSON.NUMINCRBY user:101 .profile.age 1" "path does not exist"
assert_command "GET: Verify path was not created" "JSON.GET user:101 .profile" "(nil)"
assert_command "NUMINCRBY: Fail on non-existent key" "JSON.NUMINCRBY non_key .val 1" "key or path does not exist"
assert_command "ARRINSERT: Fail on non-existent path" "JSON.ARRINSERT user:101 .hobbies 0 '\"reading\"'" "path does not exist"
assert_command "GET: Verify array path was not created" "JSON.GET user:101 .hobbies" "(nil)"

# Test 3: Verify JSON.SET still creates paths
assert_command "SET: Create new object path" "JSON.SET user:101 .address '{ \"city\": \"New York\" }'" "OK"
assert_command "GET: Verify new object path" "JSON.GET user:101 .address.city" "New York"
assert_command "SET: Create new array path" "JSON.SET user:101 .history[0] '{ \"event\": \"login\" }'" "OK"
assert_command "GET: Verify new array path" "JSON.GET user:101 .history[0].event" "login"

# Test 4: Other JSON commands
assert_command "OBJKEYS: Get keys from root" 'JSON.OBJKEYS user:101 .' "$(echo -e "name\nlogins\nbalance\nskills\naddress\nhistory")"
assert_command "ARRINSERT: Insert into existing array" "JSON.ARRINSERT user:101 .skills 0 '\"go\"'" "(integer) 3"
assert_command "GET: Verify ARRINSERT result" 'JSON.GET user:101 .skills' "$(echo -e "go\nrust\npython")"
assert_command "ARRPOP: Pop from end of array" "JSON.ARRPOP user:101 .skills" "python"
assert_command "GET: Verify ARRPOP result" 'JSON.GET user:101 .skills' "$(echo -e "go\nrust")"

echo -e "\n${GREEN}--- All JSON tests passed successfully! ---${NC}"
echo "You can now manually stop the server in the other terminal (Ctrl+C)."
