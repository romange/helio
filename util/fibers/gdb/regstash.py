#          Copyright Roman Gershman 2024.
#          Based on work by Lennart Braun 2020.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
"""
GDB extension for saving and restoring register state.

This module provides commands to push/pop register values to a per-thread stash,
which is useful for temporarily switching context to inspect fiber stacks.

Usage in GDB:
    source regstash.py
    regstash push    # Save current registers
    regstash pop     # Restore saved registers
    regstash drop    # Drop most recent entry without restoring
    regstash show    # Show stash contents
"""

import gdb
from collections import defaultdict

# Per-thread stash for register values
_reg_stash = defaultdict(list)

# Base registers that are always saved
BASE_REGISTERS = [
    "rax", "rbx", "rcx", "rdx", "rsi", "rdi", "rbp", "rsp",
    "r8", "r9", "r10", "r11", "r12", "r13", "r14", "r15",
    "rip", "eflags", "cs", "ss", "ds", "es", "fs", "gs"
]

# Extended registers for complete state
EXTRA_REGISTERS = [
    "st0", "st1", "st2", "st3", "st4", "st5", "st6", "st7",
    "fctrl", "fstat", "ftag", "fiseg", "fioff", "foseg", "fooff", "fop",
    "mxcsr"
]

# Vector registers
VECTOR_REGISTERS = [f"ymm{x}.v4_int64[{y}]" for x in range(16) for y in range(4)]

# All registers to save
ALL_REGISTERS = BASE_REGISTERS + EXTRA_REGISTERS + VECTOR_REGISTERS


def get_thread_id():
    """Get unique identifier for current thread."""
    t = gdb.selected_thread()
    return t.global_num if t else 0


def read_register(name):
    """Read a register value from GDB."""
    try:
        value = gdb.parse_and_eval(f'${name}')
        return value
    except gdb.error:
        return None


def write_register(name, value):
    """Write a value to a register in GDB."""
    hex_value = value.format_string(format='x')
    cmd = f"set ${name} = {hex_value}"
    gdb.execute(cmd)


def store_registers(tid):
    """Store all register values for the given thread."""
    values = {}
    for reg in ALL_REGISTERS:
        val = read_register(reg)
        if val is not None:
            values[reg] = val
    _reg_stash[tid].append(values)


def restore_registers(tid):
    """Restore register values for the given thread."""
    values = _reg_stash[tid].pop()
    for reg, value in values.items():
        try:
            write_register(reg, value)
        except gdb.error:
            pass  # Some registers may not be writable


class RegStash(gdb.Command):
    """Register stash commands for fiber debugging."""

    def __init__(self):
        super().__init__("regstash", gdb.COMMAND_USER, prefix=True)


class RegStashPush(gdb.Command):
    """Save current register values to the stash."""

    def __init__(self):
        super().__init__("regstash push", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        tid = get_thread_id()
        original_frame = gdb.selected_frame()
        frame0 = gdb.newest_frame()
        frame0.select()
        store_registers(tid)
        original_frame.select()
        print(f"Saved current register values of thread {tid} to stash")


class RegStashPop(gdb.Command):
    """Restore register values from the stash."""

    def __init__(self):
        super().__init__("regstash pop", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        tid = get_thread_id()
        original_frame = gdb.selected_frame()
        frame0 = gdb.newest_frame()
        frame0.select()
        try:
            restore_registers(tid)
            print(f"Restored register values of thread {tid} from stash")
        except IndexError:
            original_frame.select()
            print(f"Register stash for thread {tid} is empty")


class RegStashDrop(gdb.Command):
    """Drop the most recent entry from the stash without restoring."""

    def __init__(self):
        super().__init__("regstash drop", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        tid = get_thread_id()
        try:
            _reg_stash[tid].pop()
            print(f"Dropped most recent register stash entry for thread {tid}")
        except IndexError:
            print(f"Register stash for thread {tid} is empty")


class RegStashShow(gdb.Command):
    """Show an overview of the register stash."""

    def __init__(self):
        super().__init__("regstash show", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        tid = get_thread_id()
        entries = _reg_stash[tid]
        print(f"{len(entries)} entries for thread {tid} in the stash")
        if not entries:
            return
        values = entries[-1]
        print("Most recent register values:")
        for register in BASE_REGISTERS:
            if register in values:
                print(f"  {register:8s}    {values[register].format_string(format='x')}")


# Register the commands
RegStash()
RegStashPush()
RegStashPop()
RegStashDrop()
RegStashShow()
