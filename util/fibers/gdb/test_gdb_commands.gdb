# GDB test script for helio fiber debugging extensions
# This script tests that the GDB Python commands load and work correctly.
#
# Usage:
#   gdb -batch -x test_gdb_commands.gdb ./gdb_fiber_test
#
# Or interactively:
#   gdb ./gdb_fiber_test
#   (gdb) source test_gdb_commands.gdb

# Set up test environment
set pagination off
set confirm off

# Load the helio GDB extensions
# Note: The path should be relative to the build directory
python
import sys
import os
# Add the source directory to Python path
script_dir = os.environ.get('HELIO_SOURCE_DIR', '../util/fibers/gdb')
if os.path.exists(script_dir):
    sys.path.insert(0, script_dir)
end

# Source the extensions (try multiple possible paths)
python
import gdb
import os

paths_to_try = [
    '../util/fibers/gdb/helio_gdb.py',     # From build directory
    'util/fibers/gdb/helio_gdb.py',        # From source root
    './helio_gdb.py',                       # Current directory
]

loaded = False
for path in paths_to_try:
    if os.path.exists(path):
        gdb.execute(f'source {path}')
        loaded = True
        break

if not loaded:
    # Try loading individual files
    for path in ['../util/fibers/gdb/regstash.py', 'util/fibers/gdb/regstash.py']:
        if os.path.exists(path):
            gdb.execute(f'source {path}')
            break
    for path in ['../util/fibers/gdb/helio_fiber.py', 'util/fibers/gdb/helio_fiber.py']:
        if os.path.exists(path):
            gdb.execute(f'source {path}')
            break

print("GDB extension loading complete")
end

# Test that commands are registered
echo \n=== Testing command registration ===\n
help helio
help regstash

# Run the test program to a point where fibers exist
echo \n=== Starting test program ===\n
break GdbFiberTest_MultipleFibers_Test::TestBody
run --gtest_filter=GdbFiberTest.MultipleFibers

# When stopped, test the fiber commands
echo \n=== Testing helio fiber commands ===\n

# Continue past fiber creation
continue

# Test fiber list command
echo \n--- Testing 'helio fiber list' ---\n
helio fiber list

# Test scheduler command
echo \n--- Testing 'helio fiber scheduler' ---\n
helio fiber scheduler

# Test active fiber command
echo \n--- Testing 'helio fiber active' ---\n
helio fiber active

# Test regstash commands
echo \n--- Testing regstash commands ---\n
regstash show
regstash push
regstash show
regstash pop

# Continue to completion
echo \n=== Continuing to completion ===\n
continue

echo \n=== GDB extension tests completed ===\n
quit
