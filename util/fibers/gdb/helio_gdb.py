#          Copyright Roman Gershman 2024.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
"""
Helio fiber-aware GDB extension loader.

This is the main entry point for loading helio fiber debugging support in GDB.

Usage:
    source helio_gdb.py

Or add to your ~/.gdbinit:
    source /path/to/helio/util/fibers/gdb/helio_gdb.py

Available commands after loading:
    regstash push          - Save current register values
    regstash pop           - Restore saved register values
    regstash drop          - Drop most recent stash entry
    regstash show          - Show stash contents

    helio fiber list       - List all fibers in current scheduler
    helio fiber info <ptr> - Show detailed info about a fiber
    helio fiber bt <ptr>   - Show backtrace for a fiber
    helio fiber bt-all     - Show backtraces for all fibers
    helio fiber switch <ptr> - Switch GDB context to a fiber
    helio fiber scheduler  - Show scheduler information
    helio fiber active     - Show currently active fiber
"""

import os
import gdb

# Get the directory containing this script
_script_dir = os.path.dirname(os.path.abspath(__file__))

# Source the helper scripts
gdb.execute(f"source {os.path.join(_script_dir, 'regstash.py')}")
gdb.execute(f"source {os.path.join(_script_dir, 'helio_fiber.py')}")

print("Helio GDB extensions loaded successfully.")
print("Type 'help helio' for available commands.")
