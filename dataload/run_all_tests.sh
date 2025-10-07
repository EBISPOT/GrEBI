#!/usr/bin/env bash
# Master test runner for all GrEBI Rust programs
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED_TESTS=()
PASSED_TESTS=()
TOTAL_TESTS=0

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║    GrEBI Rust Dataload Programs Test Suite                ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Find all test.sh files
TEST_SCRIPTS=$(find . -name "test.sh" -path "*/tests/test.sh" | sort)

for test_script in $TEST_SCRIPTS; do
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    test_dir=$(dirname "$test_script")
    program_name=$(basename "$(dirname "$(dirname "$test_script")")")
    
    echo -e "${BLUE}[$TOTAL_TESTS]${NC} Running tests for ${BLUE}$program_name${NC}..."
    
    if (cd "$test_dir" && bash test.sh) 2>&1 | sed 's/^/    /'; then
        PASSED_TESTS+=("$program_name")
        echo -e "    ${GREEN}✓ PASSED${NC}"
    else
        FAILED_TESTS+=("$program_name")
        echo -e "    ${RED}✗ FAILED${NC}"
    fi
    echo ""
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║    Test Summary                                            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Total tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: ${#PASSED_TESTS[@]}${NC}"
echo -e "${RED}Failed: ${#FAILED_TESTS[@]}${NC}"

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo ""
    echo -e "${RED}Failed tests:${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "  ${RED}✗${NC} $test"
    done
    exit 1
else
    echo ""
    echo -e "${GREEN}All tests passed! 🎉${NC}"
    exit 0
fi
