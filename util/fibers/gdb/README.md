# Helio Fiber GDB Debugging Extensions

This directory contains GDB Python scripts for fiber-aware debugging in helio.

## Overview

When debugging applications using helio's fiber-based concurrency, standard GDB
commands only show the context of the currently running fiber. These extensions
allow you to:

- List all fibers in a thread's scheduler
- View stack traces for suspended fibers
- Switch between fiber contexts
- Inspect fiber state (ready, sleeping, terminated)

## Requirements

- GDB 8.0+ with Python 3 support
- A debug build of your helio-based application

## Usage

### Loading the Extension

In GDB, source the main loader script:

```gdb
(gdb) source /path/to/helio/util/fibers/gdb/helio_gdb.py
```

Or add to your `~/.gdbinit`:

```
source /path/to/helio/util/fibers/gdb/helio_gdb.py
```

### Available Commands

#### Fiber Commands

- **`helio fiber list`** - List all fibers in the current thread's scheduler
  ```gdb
  (gdb) helio fiber list
  Found 3 fiber(s):
  --------------------------------------------------------------------------------
    0x7fffd8000b70  MAIN      main                                        
    0x7fffd8001000  DISPATCH  _dispatch                                   
    0x7fffd8002000  WORKER    my_fiber                                    
  --------------------------------------------------------------------------------
  ```

- **`helio fiber info <ptr>`** - Show detailed information about a fiber
  ```gdb
  (gdb) helio fiber info 0x7fffd8002000
  Fiber: 0x7fffd8002000
    Name: my_fiber
    Type: WORKER
    Flags: 0x0
    Stack size: 65536
    Stack bottom: 0x7fffd8002200
    Use count: 1
    Preempt count: 5
    State: [RUNNING/READY]
  ```

- **`helio fiber bt <ptr>`** - Show backtrace for a suspended fiber
  ```gdb
  (gdb) helio fiber bt 0x7fffd8002000
  === Backtrace for fiber 'my_fiber' ===
  #0  0x00007ffff7e8a... in __GI___libc_nanosleep
  #1  0x00000000004... in util::fb2::ThisFiber::SleepFor
  ...
  ```

- **`helio fiber bt-all`** - Show backtraces for all fibers

- **`helio fiber switch <ptr>`** - Switch GDB context to a fiber (modifies registers!)
  ```gdb
  (gdb) regstash push       # Save current context first!
  (gdb) helio fiber switch 0x7fffd8002000
  Switched to fiber context 'my_fiber'
  (gdb) bt                  # Now shows this fiber's stack
  (gdb) regstash pop        # Restore original context
  ```

- **`helio fiber scheduler`** - Show current thread's scheduler information

- **`helio fiber active`** - Show the currently active fiber

#### Register Stash Commands

The register stash is used to save/restore GDB's view of registers when
switching between fiber contexts:

- **`regstash push`** - Save current register values to the stash
- **`regstash pop`** - Restore register values from the stash
- **`regstash drop`** - Drop most recent entry without restoring
- **`regstash show`** - Show stash contents and most recent values

## Example Debugging Session

```gdb
# Start your application and hit a breakpoint
(gdb) run
Breakpoint 1, main () at my_app.cc:100

# Load the extension
(gdb) source helio_gdb.py
Helio fiber GDB extension loaded.

# List all fibers
(gdb) helio fiber list
Found 5 fiber(s):
  0x...  MAIN      main
  0x...  DISPATCH  _dispatch
  0x...  WORKER    http_handler
  0x...  WORKER    db_worker
  0x...  WORKER    timer_fiber

# Get backtrace for a suspended fiber
(gdb) helio fiber bt 0x...
=== Backtrace for fiber 'db_worker' ===
#0  ... in util::fb2::detail::FiberInterface::SwitchTo
#1  ... in DatabaseClient::Query
#2  ... in ProcessRequest
...

# Switch to a fiber to examine local variables
(gdb) regstash push
Saved current register values to stash
(gdb) helio fiber switch 0x...
Switched to fiber context 'db_worker'
(gdb) info locals
query = "SELECT * FROM users"
result_count = 42
(gdb) regstash pop
Restored register values from stash
```

## Implementation Notes

### How It Works

Helio fibers use `boost::context` for context switching. Each fiber has a saved
CPU state (registers, stack pointer) stored in its `entry_` member when suspended.
These extensions read that saved state to reconstruct stack traces for suspended
fibers.

### Fiber Types

- **MAIN**: The main thread's "fiber" representing native execution
- **DISPATCH**: The scheduler's dispatch fiber that runs the event loop
- **WORKER**: User-created fibers

### Limitations

1. **x86_64 only**: Register layouts are currently hardcoded for x86_64
2. **Debug builds recommended**: Optimized builds may inline functions
3. **Active fiber**: The currently running fiber's state is in live registers,
   not in its `entry_` field, so `helio fiber bt` may not work for it

## Troubleshooting

### "Could not find scheduler"
Make sure you're in a thread that has initialized fibers. The main thread
should have a scheduler after the first fiber-related operation.

### "Cannot get context for fiber"
The fiber's `entry_` field may be empty if:
- It's the currently active fiber (use normal `bt` instead)
- It's being constructed/destructed

### Incorrect backtraces
Ensure you're using a debug build (`-DCMAKE_BUILD_TYPE=Debug`) and that
debug symbols are loaded.

## Files

- `helio_gdb.py` - Main loader script
- `helio_fiber.py` - Fiber inspection and debugging commands
- `regstash.py` - Register stash utility for context switching
