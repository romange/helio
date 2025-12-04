#          Copyright Roman Gershman 2024.
#          Based on work by Lennart Braun 2020, Niek Bouman 2021.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
"""
GDB extension for fiber-aware debugging in helio.

This module provides commands to inspect and debug fibers in the helio framework.
It allows listing all fibers, switching between fiber contexts, and getting
backtraces for suspended fibers.

Usage in GDB:
    source helio_fiber.py
    helio fiber list           # List all fibers
    helio fiber info <ptr>     # Show info about a fiber
    helio fiber bt <ptr>       # Show backtrace of a fiber
    helio fiber switch <ptr>   # Switch to a fiber's context
    helio fiber scheduler      # Show scheduler info
"""

import gdb


def write_register(name, value):
    """Write value to register."""
    hex_value = value.format_string(format='x')
    cmd = f"set ${name} = {hex_value}"
    gdb.execute(cmd)


class FContext:
    """
    Representation of boost::context::detail::fcontext.

    The fcontext structure stores the saved CPU registers when a fiber
    is suspended. The layout depends on the platform (x86_64/ARM).
    """

    def __init__(self, ptr):
        self.ptr = ptr

    def read_registers(self):
        """
        Read saved registers from the fcontext.

        The register layout for x86_64 boost::context with fcontext_t:
        - Offset 0: hidden control word (x87/MXCSR)
        - Offset 8-48: r12, r13, r14, r15, rbx, rbp (callee-saved)
        - Offset 56: rip (return address)
        - Stack pointer (rsp) is the fcontext pointer itself
        """
        values = {}
        try:
            t = gdb.lookup_type('uint64_t').array(8)
            registers = self.ptr.cast(t)
            # Skip x87 control word at index 0
            values['r12'] = registers[1]
            values['r13'] = registers[2]
            values['r14'] = registers[3]
            values['r15'] = registers[4]
            values['rbx'] = registers[5]
            values['rbp'] = registers[6]
            values['rip'] = registers[7]
            values['rsp'] = self.ptr
        except gdb.error as e:
            print(f"Error reading registers: {e}")
        return values


class FiberInterface:
    """
    Representation of util::fb2::detail::FiberInterface.

    This class wraps a pointer to a FiberInterface object and provides
    methods to inspect its state and switch to its context.
    """

    # Type constants matching FiberInterface::Type enum
    TYPE_MAIN = 0
    TYPE_DISPATCH = 1
    TYPE_WORKER = 2

    TYPE_NAMES = {
        TYPE_MAIN: "MAIN",
        TYPE_DISPATCH: "DISPATCH",
        TYPE_WORKER: "WORKER"
    }

    def __init__(self, ptr, raw=False):
        """
        Initialize FiberInterface wrapper.

        Args:
            ptr: GDB value or integer address
            raw: If True, ptr is an integer address to be cast
        """
        if raw:
            try:
                fi_p = gdb.lookup_type("util::fb2::detail::FiberInterface").pointer()
                ptr = gdb.Value(ptr).cast(fi_p)
            except gdb.error:
                print("Warning: Could not cast to FiberInterface pointer")
        self.ptr = ptr

    def get(self):
        """Return the dereferenced FiberInterface object."""
        return self.ptr.dereference()

    def get_ptr(self):
        """Return the pointer value."""
        return self.ptr

    def get_name(self):
        """Get the fiber name."""
        try:
            name_arr = self.ptr.dereference()['name_']
            return name_arr.string()
        except gdb.error:
            return "<unknown>"

    def get_type(self):
        """Get the fiber type as integer."""
        try:
            return int(self.ptr.dereference()['type_'])
        except gdb.error:
            return -1

    def get_type_name(self):
        """Get the fiber type as string."""
        return self.TYPE_NAMES.get(self.get_type(), "UNKNOWN")

    def get_flags(self):
        """Get the fiber flags."""
        try:
            return int(self.ptr.dereference()['flags_'])
        except gdb.error:
            return 0

    def get_stack_size(self):
        """Get the fiber stack size."""
        try:
            return int(self.ptr.dereference()['stack_size_'])
        except gdb.error:
            return 0

    def get_stack_bottom(self):
        """Get the fiber stack bottom address."""
        try:
            return int(self.ptr.dereference()['stack_bottom_'])
        except gdb.error:
            return 0

    def get_use_count(self):
        """Get the reference count."""
        try:
            return int(self.ptr.dereference()['use_count_'])
        except gdb.error:
            return 0

    def get_preempt_count(self):
        """Get the preempt count."""
        try:
            return int(self.ptr.dereference()['preempt_cnt_'])
        except gdb.error:
            return 0

    def is_terminated(self):
        """Check if fiber is terminated."""
        return (self.get_flags() & 0x1) != 0

    def is_busy(self):
        """Check if fiber is busy."""
        return (self.get_flags() & 0x2) != 0

    def get_entry(self):
        """Get the boost::context::fiber_context entry point."""
        try:
            return self.ptr.dereference()['entry_']
        except gdb.error:
            return None

    def get_fctx(self):
        """Get the FContext for this fiber."""
        try:
            entry = self.get_entry()
            if entry is None:
                return None
            # The fiber_context stores fctx_ internally
            # This depends on boost::context implementation
            fctx = entry['fctx_']
            return FContext(fctx)
        except gdb.error as e:
            print(f"Error getting fctx: {e}")
            return None

    def get_scheduler(self):
        """Get the scheduler for this fiber."""
        try:
            sched_ptr = self.ptr.dereference()['scheduler_']
            return Scheduler(sched_ptr)
        except gdb.error:
            return None

    def is_list_linked(self):
        """Check if fiber is in a ready/terminate queue."""
        try:
            hook = self.ptr.dereference()['list_hook']
            # For boost::intrusive safe_link mode, check if next_ pointer
            # is non-null (linked hooks have valid pointers)
            next_ptr = hook['next_']
            # A linked hook has a non-null next pointer
            return int(next_ptr) != 0
        except gdb.error:
            return False

    def is_sleep_linked(self):
        """Check if fiber is in sleep queue."""
        try:
            hook = self.ptr.dereference()['sleep_hook']
            # For boost::intrusive set hooks, check parent/left/right pointers
            # A linked hook will have at least one non-null pointer
            parent = hook['parent_']
            return int(parent) != 0
        except gdb.error:
            return False

    def backtrace(self):
        """Print backtrace for this fiber."""
        fctx = self.get_fctx()
        if fctx is None:
            print(f"Cannot get context for fiber '{self.get_name()}'")
            return

        regs = fctx.read_registers()
        if not regs:
            print("Could not read registers")
            return

        original_frame = gdb.selected_frame()

        # Save current registers
        gdb.execute('regstash push')

        # Switch to newest frame and set fiber's registers
        gdb.newest_frame().select()
        for reg, val in regs.items():
            write_register(reg, val)

        print(f"\n=== Backtrace for fiber '{self.get_name()}' ===")
        gdb.execute('bt')

        # Restore original registers
        gdb.execute('regstash pop')
        original_frame.select()

    def switch(self):
        """Switch GDB context to this fiber."""
        fctx = self.get_fctx()
        if fctx is None:
            print(f"Cannot get context for fiber '{self.get_name()}'")
            return

        regs = fctx.read_registers()
        if not regs:
            print("Could not read registers")
            return

        gdb.newest_frame().select()
        for reg, val in regs.items():
            write_register(reg, val)
        print(f"Switched to fiber context '{self.get_name()}'")

    def print_info(self):
        """Print detailed information about this fiber."""
        print(f"Fiber: {self.ptr}")
        print(f"  Name: {self.get_name()}")
        print(f"  Type: {self.get_type_name()}")
        print(f"  Flags: 0x{self.get_flags():x}")
        print(f"  Stack size: {self.get_stack_size()}")
        print(f"  Stack bottom: 0x{self.get_stack_bottom():x}")
        print(f"  Use count: {self.get_use_count()}")
        print(f"  Preempt count: {self.get_preempt_count()}")

        state = []
        if self.is_terminated():
            state.append("TERMINATED")
        if self.is_busy():
            state.append("BUSY")
        if not state:
            state.append("RUNNING/READY")
        print(f"  State: [{', '.join(state)}]")

    def __str__(self):
        return f"FiberInterface({self.ptr}) name='{self.get_name()}'"


class Scheduler:
    """
    Representation of util::fb2::detail::Scheduler.

    The scheduler manages fiber queues, switching, and scheduling.
    """

    def __init__(self, ptr):
        self.ptr = ptr

    def get(self):
        """Return the dereferenced Scheduler object."""
        return self.ptr.dereference()

    def get_ptr(self):
        """Return the pointer value."""
        return self.ptr

    def get_main_context(self):
        """Get the main fiber context."""
        try:
            ctx = self.ptr.dereference()['main_cntx_']
            return FiberInterface(ctx)
        except gdb.error:
            return None

    def get_dispatch_context(self):
        """Get the dispatch fiber context."""
        try:
            # dispatch_cntx_ is a boost::intrusive_ptr
            ptr = self.ptr.dereference()['dispatch_cntx_']['px']
            return FiberInterface(ptr)
        except gdb.error:
            return None

    def get_num_workers(self):
        """Get the number of worker fibers."""
        try:
            return int(self.ptr.dereference()['num_worker_fibers_'])
        except gdb.error:
            return 0

    def get_worker_stack_size(self):
        """Get the total worker stack size."""
        try:
            return int(self.ptr.dereference()['worker_stack_size_'])
        except gdb.error:
            return 0

    def is_shutdown(self):
        """Check if scheduler is shutting down."""
        try:
            return bool(self.ptr.dereference()['shutdown_'])
        except gdb.error:
            return False

    def get_fibers_list(self):
        """
        Get all fibers in this scheduler's fibers_ list.

        Returns a list of FiberInterface objects.
        """
        fibers = []
        try:
            # fibers_ is a boost::intrusive::list
            fibers_list = self.ptr.dereference()['fibers_']
            # Get the header node
            header = fibers_list['data_']['root_plus_size_']['m_header']
            node = header['next_']
            end_node = header.address

            # The offset from fibers_hook to FiberInterface beginning
            # We need to compute this from the member offset
            char_p = gdb.lookup_type('char').pointer()
            fi_p = gdb.lookup_type("util::fb2::detail::FiberInterface").pointer()

            # Get offset of fibers_hook in FiberInterface
            offset = None
            try:
                # Try to get offset using gdb's offsetof-like expression
                offset = int(gdb.parse_and_eval(
                    "&((util::fb2::detail::FiberInterface*)0)->fibers_hook"
                ))
            except gdb.error:
                pass

            if offset is None:
                # Alternative: try to compute offset from type info
                try:
                    fi_type = gdb.lookup_type("util::fb2::detail::FiberInterface")
                    for field in fi_type.fields():
                        if field.name == 'fibers_hook':
                            offset = field.bitpos // 8
                            break
                except gdb.error:
                    pass

            if offset is None:
                # Last resort fallback with warning
                print("Warning: Could not determine fibers_hook offset, using estimated value")
                offset = 128  # Approximate offset based on struct layout

            max_iters = 1000  # Safety limit
            iters = 0
            while node != end_node and iters < max_iters:
                # Convert node to FiberInterface pointer
                fi_ptr = (node.cast(char_p) - offset).cast(fi_p)
                fibers.append(FiberInterface(fi_ptr))
                node = node['next_']
                iters += 1

        except gdb.error as e:
            print(f"Error iterating fibers list: {e}")

        return fibers

    def print_info(self):
        """Print scheduler information."""
        print(f"Scheduler: {self.ptr}")
        print(f"  Shutdown: {self.is_shutdown()}")
        print(f"  Worker fibers: {self.get_num_workers()}")
        print(f"  Worker stack size: {self.get_worker_stack_size()}")

        main = self.get_main_context()
        if main:
            print(f"  Main context: {main}")

        dispatch = self.get_dispatch_context()
        if dispatch:
            print(f"  Dispatch context: {dispatch}")

    @staticmethod
    def find():
        """Find the current thread's scheduler."""
        try:
            active = gdb.parse_and_eval("util::fb2::detail::FiberActive()")
            if active:
                fiber = FiberInterface(active)
                return fiber.get_scheduler()
        except gdb.error:
            pass
        return None

    def __str__(self):
        return f"Scheduler({self.ptr})"


# GDB Commands

class HelioCommand(gdb.Command):
    """Collection of helio debugging commands."""

    def __init__(self):
        super().__init__("helio", gdb.COMMAND_USER, prefix=True)


class HelioFiberCommand(gdb.Command):
    """Commands for fiber debugging."""

    def __init__(self):
        super().__init__("helio fiber", gdb.COMMAND_USER, prefix=True)


class HelioFiberListCommand(gdb.Command):
    """List all fibers in the current thread's scheduler."""

    def __init__(self):
        super().__init__("helio fiber list", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        scheduler = Scheduler.find()
        if scheduler is None:
            print("Could not find scheduler for current thread")
            return

        fibers = scheduler.get_fibers_list()
        if not fibers:
            print("No fibers found")
            return

        print(f"Found {len(fibers)} fiber(s):")
        print("-" * 80)
        for fb in fibers:
            state = []
            if fb.is_terminated():
                state.append("TERM")
            if fb.is_busy():
                state.append("BUSY")
            state_str = f"[{','.join(state)}]" if state else ""

            print(f"  {fb.ptr}  {fb.get_type_name():8s}  {fb.get_name():20s}  {state_str}")
        print("-" * 80)


class HelioFiberInfoCommand(gdb.Command):
    """Show detailed information about a fiber.

    Usage: helio fiber info <pointer>
    """

    def __init__(self):
        super().__init__("helio fiber info", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if len(argv) != 1:
            print("Usage: helio fiber info <pointer>")
            return

        try:
            ptr = int(argv[0], 0)
            fiber = FiberInterface(ptr, raw=True)
            fiber.print_info()
        except ValueError:
            print(f"Invalid pointer: {argv[0]}")


class HelioFiberBtCommand(gdb.Command):
    """Show backtrace for a fiber.

    Usage: helio fiber bt <pointer>
    """

    def __init__(self):
        super().__init__("helio fiber bt", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if len(argv) != 1:
            print("Usage: helio fiber bt <pointer>")
            return

        try:
            ptr = int(argv[0], 0)
            fiber = FiberInterface(ptr, raw=True)
            fiber.backtrace()
        except ValueError:
            print(f"Invalid pointer: {argv[0]}")


class HelioFiberSwitchCommand(gdb.Command):
    """Switch GDB context to a fiber.

    Usage: helio fiber switch <pointer>

    Warning: This modifies register state. Use 'regstash push' first
    to save current state, and 'regstash pop' to restore.
    """

    def __init__(self):
        super().__init__("helio fiber switch", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if len(argv) != 1:
            print("Usage: helio fiber switch <pointer>")
            return

        try:
            ptr = int(argv[0], 0)
            fiber = FiberInterface(ptr, raw=True)
            fiber.switch()
        except ValueError:
            print(f"Invalid pointer: {argv[0]}")


class HelioFiberSchedulerCommand(gdb.Command):
    """Show information about the current thread's scheduler."""

    def __init__(self):
        super().__init__("helio fiber scheduler", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        scheduler = Scheduler.find()
        if scheduler is None:
            print("Could not find scheduler for current thread")
            return

        scheduler.print_info()


class HelioFiberActiveCommand(gdb.Command):
    """Show information about the currently active fiber."""

    def __init__(self):
        super().__init__("helio fiber active", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        try:
            active = gdb.parse_and_eval("util::fb2::detail::FiberActive()")
            if active:
                fiber = FiberInterface(active)
                fiber.print_info()
            else:
                print("No active fiber found")
        except gdb.error as e:
            print(f"Could not get active fiber: {e}")


class HelioFiberBtAllCommand(gdb.Command):
    """Show backtraces for all fibers in the current scheduler."""

    def __init__(self):
        super().__init__("helio fiber bt-all", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        scheduler = Scheduler.find()
        if scheduler is None:
            print("Could not find scheduler for current thread")
            return

        fibers = scheduler.get_fibers_list()
        if not fibers:
            print("No fibers found")
            return

        print(f"Printing backtraces for {len(fibers)} fiber(s)...")
        for fb in fibers:
            print("\n" + "=" * 80)
            fb.print_info()
            fb.backtrace()


# Register all commands
HelioCommand()
HelioFiberCommand()
HelioFiberListCommand()
HelioFiberInfoCommand()
HelioFiberBtCommand()
HelioFiberSwitchCommand()
HelioFiberSchedulerCommand()
HelioFiberActiveCommand()
HelioFiberBtAllCommand()

print("Helio fiber GDB extension loaded.")
print("Commands: helio fiber [list|info|bt|bt-all|switch|scheduler|active]")
