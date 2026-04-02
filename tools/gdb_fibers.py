"""
GDB extension for debugging Helio fibers.

Usage:
    (gdb) source tools/gdb_fibers.py
    (gdb) fibers                  # list all fibers in current thread
    (gdb) fiber-bt <addr|name>    # show backtrace of a fiber
    (gdb) fiber-switch <addr|name>  # switch register view to a fiber
    (gdb) fiber-restore           # restore original register view

Works with both live debugging and core dumps (no inferior calls needed).
Supports x86_64 and aarch64.
"""

import gdb

# ── boost::context fcontext register save layouts ──────────────────────
#
# When a fiber is suspended, entry_.fctx_ points to the saved area on the
# fiber's stack.  The layout differs by architecture.

_ARCH_CONFIGS = {
    "x86_64": {
        # See jump_x86_64_sysv_elf_gas.S in libboost_context.
        "regs": {
            "r12": 0x10,
            "r13": 0x18,
            "r14": 0x20,
            "r15": 0x28,
            "rbx": 0x30,
            "rbp": 0x38,
            "rip": 0x40,  # return address (pushed by call instruction)
        },
        "frame_size": 0x48,  # 0x40 registers + 0x08 return addr
        "sp": "rsp",
        "pc": "rip",
        "fp": "rbp",
        "callee_saved": ("rsp", "rbp", "rip", "r12", "r13", "r14", "r15", "rbx"),
    },
    "aarch64": {
        # See jump_arm64_aapcs_elf_gas.S in libboost_context.
        # sub sp, sp, #0xb0 ; then pairs stored with stp:
        #   d8-d15  at 0x00..0x3f   (FPU callee-saved)
        #   x19-x28 at 0x40..0x8f   (integer callee-saved)
        #   x29,x30 at 0x90         (FP, LR)
        #   x30     at 0xa0         (PC / return addr for ret x4)
        "regs": {
            "d8":  0x00, "d9":  0x08,
            "d10": 0x10, "d11": 0x18,
            "d12": 0x20, "d13": 0x28,
            "d14": 0x30, "d15": 0x38,
            "x19": 0x40, "x20": 0x48,
            "x21": 0x50, "x22": 0x58,
            "x23": 0x60, "x24": 0x68,
            "x25": 0x70, "x26": 0x78,
            "x27": 0x80, "x28": 0x88,
            "x29": 0x90, "x30": 0x98,
            "pc":  0xa0,  # separate copy of LR used as return address
        },
        "frame_size": 0xb0,
        "sp": "sp",
        "pc": "pc",
        "fp": "x29",
        "callee_saved": (
            "sp", "pc", "x29", "x30",
            "x19", "x20", "x21", "x22", "x23", "x24",
            "x25", "x26", "x27", "x28",
            "d8", "d9", "d10", "d11", "d12", "d13", "d14", "d15",
        ),
    },
}


def _get_arch():
    """Detect target architecture using the GDB Python API.

    Queries the current frame or inferior for the architecture name,
    avoiding CLI parsing and static caching at import time.
    """
    try:
        name = gdb.newest_frame().architecture().name()
    except Exception:
        name = gdb.selected_inferior().architecture().name()

    if name.startswith("aarch64"):
        return "aarch64"
    if "x86-64" in name or name.startswith("i386:x86-64"):
        return "x86_64"
    raise gdb.GdbError(f"Unsupported architecture: {name}")

TYPE_NAMES = {0: "MAIN", 1: "DISPATCH", 2: "WORKER"}
PRIO_NAMES = {0: "NORMAL", 1: "BKGND", 2: "HIGH"}

# Saved register state for fiber-switch/fiber-restore, keyed by GDB thread id.
_saved_contexts = {}


def _read_u64(addr):
    """Read a uint64_t from an address."""
    return int(gdb.Value(addr).cast(gdb.lookup_type("uint64_t").pointer()).dereference())


def _read_ptr(addr):
    """Read a pointer from an address."""
    return _read_u64(addr)


def _get_fb_initializer():
    """Get the thread-local TL_FiberInitializer for the current thread.

    Works on core dumps (reads memory directly, no inferior calls).
    When GDB stops in a frame without helio debug info (e.g., epoll_wait),
    direct symbol access fails with "unknown type". We work around this
    by taking the address of the mangled TLS symbol (always works) and
    casting it to the type obtained via gdb.lookup_type().
    """
    _MANGLED = "'_ZZN4util3fb26detail13FbInitializerEvE14fb_initializer'"
    _DEMANGLED = "'util::fb2::detail::FbInitializer()::fb_initializer'"
    _TYPE = "util::fb2::detail::TL_FiberInitializer"

    # Fast path: works when the current frame has helio debug info in scope.
    for sym in (_DEMANGLED, _MANGLED):
        try:
            fb_init = gdb.parse_and_eval(sym)
            # If the type resolved correctly, we're done.
            if str(fb_init.type) != "<data variable, no debug info>":
                return fb_init
        except gdb.error:
            pass

    # Slow path: frame lacks helio types (e.g., thread stopped in libc).
    # Get the TLS address via &symbol, then cast via lookup_type.
    try:
        addr_val = gdb.parse_and_eval(f"&{_MANGLED}")
        tls_type = gdb.lookup_type(_TYPE)
        return addr_val.cast(tls_type.pointer()).dereference()
    except gdb.error:
        pass

    raise gdb.GdbError(
        "Cannot find TL_FiberInitializer. "
        "Is the binary built with debug info and linked against helio fibers?"
    )


def _get_hook_offset(hook_name):
    """Get the byte offset of a member hook within FiberInterface."""
    fi_type = gdb.lookup_type("util::fb2::detail::FiberInterface")
    for field in fi_type.fields():
        if field.name == hook_name:
            return field.bitpos // 8
    raise gdb.GdbError(f"Cannot find '{hook_name}' field in FiberInterface")


def _hook_is_linked(fiber_val, hook_name):
    """Check if an intrusive hook is linked (node.next_ != nullptr)."""
    hook = fiber_val[hook_name]
    # The hook inherits from generic_hook which contains a list_node.
    # We need to dig into its internal node to check next_.
    # The structure is: hook -> node_ (or the hook IS the node via inheritance).
    # For safe_link, unlinked means next_ == nullptr.
    try:
        # Try accessing the internal node. The exact path depends on boost version.
        # Common paths: hook -> node_algorithms type has node with next_/prev_.
        # The hook itself is effectively a list_node with next_ and prev_ fields.
        # Let's read raw memory: the hook is a list_node{next_, prev_} at its address.
        hook_addr = int(hook.address)
        next_ptr = _read_ptr(hook_addr)
        return next_ptr != 0
    except Exception:
        return False


def _walk_fiber_list(sched_ptr):
    """Walk Scheduler::fibers_ intrusive list. Yields FiberInterface addresses."""
    hook_offset = _get_hook_offset("fibers_hook")
    fi_ptr_type = gdb.lookup_type("util::fb2::detail::FiberInterface").pointer()

    # Scheduler::fibers_ is a boost::intrusive::list. Its layout is:
    #   data_ -> root_plus_size_ -> m_header (which is a list_node{next_, prev_})
    # The m_header acts as sentinel. We need its address and then follow next_ pointers.
    sched_val = sched_ptr.dereference()
    fibers_list = sched_val["fibers_"]

    # Navigate to the sentinel node. The internal structure path is:
    #   fibers_.data_.root_plus_size_.m_header
    # m_header inherits from list_node, so its first two members are next_ and prev_.
    try:
        sentinel = fibers_list["data_"]["root_plus_size_"]["m_header"]
    except gdb.error:
        raise gdb.GdbError(
            "Cannot navigate boost::intrusive::list internals. "
            "Boost version may differ from expected layout."
        )

    sentinel_addr = int(sentinel.address)

    # The sentinel's next_ is the first element's fibers_hook.
    # Read next_ (first field of list_node).
    current_hook_addr = _read_ptr(sentinel_addr)

    while current_hook_addr != sentinel_addr and current_hook_addr != 0:
        fiber_addr = current_hook_addr - hook_offset
        yield gdb.Value(fiber_addr).cast(fi_ptr_type)
        # Follow next_ pointer (first field of the hook/node).
        current_hook_addr = _read_ptr(current_hook_addr)


def _read_atomic(val):
    """Read a std::atomic value, handling different stdlib implementations."""
    # libstdc++: std::atomic has _M_i member
    # libc++: std::atomic has __a_ -> __a_value member
    # Direct int() cast works in some GDB versions.
    try:
        return int(val["_M_i"])
    except gdb.error:
        pass
    try:
        return int(val["__a_"]["__a_value"])
    except gdb.error:
        pass
    return int(val)


def _fiber_state(fiber_val, fiber_addr, active_addr):
    """Determine the state of a fiber. Returns a string."""
    if fiber_addr == active_addr:
        return "active"

    flags = _read_atomic(fiber_val["flags_"])
    if flags & 0x1:
        return "terminated"

    # Check if in ready queue (list_hook linked).
    if _hook_is_linked(fiber_val, "list_hook"):
        return "ready"

    # Check if sleeping (sleep_hook linked).
    if _hook_is_linked(fiber_val, "sleep_hook"):
        return "sleeping"

    return "suspended"


def _fiber_name(fiber_val):
    """Get fiber name as a Python string."""
    try:
        return fiber_val["name_"].string()
    except Exception:
        return "<unknown>"


def _get_entry_offset():
    """Get byte offset of entry_ within FiberInterface.

    entry_ is the first DATA member but FiberInterface has a vtable pointer
    (virtual destructor), so entry_ is typically at offset 8.
    We use GDB's type info to be safe.
    """
    fi_type = gdb.lookup_type("util::fb2::detail::FiberInterface")
    for field in fi_type.fields():
        if field.name == "entry_":
            return field.bitpos // 8
    raise gdb.GdbError("Cannot find 'entry_' field in FiberInterface")


def _read_fctx(fiber_addr):
    """Read the fcontext_t (saved RSP) from a fiber's entry_ field.

    entry_ is a boost::context::fiber_context whose sole member fctx_ (void*)
    holds the saved stack pointer.
    """
    entry_offset = _get_entry_offset()
    fctx = _read_ptr(fiber_addr + entry_offset)
    return fctx


def _read_saved_regs(fctx):
    """Read saved registers from the fcontext save area. Returns a dict."""
    if fctx == 0:
        return None
    cfg = _ARCH_CONFIGS[_get_arch()]
    regs = {}
    for name, offset in cfg["regs"].items():
        regs[name] = _read_u64(fctx + offset)
    # SP is not saved explicitly; it equals fctx + frame_size (the stack
    # pointer value right before jump_fcontext was called).
    regs[cfg["sp"]] = fctx + cfg["frame_size"]
    return regs


def _read_reg_raw(name):
    """Read a GDB register as a uint64_t.

    For integer registers, int(gdb.parse_and_eval("$reg")) works directly.
    For aarch64 FPU d-registers the GDB Value has a struct type
    {f, u, s}; we read the '.u' (unsigned) member.
    """
    try:
        return int(gdb.parse_and_eval(f"${name}"))
    except (gdb.error, TypeError):
        # aarch64 d-register: access the unsigned member.
        return int(gdb.parse_and_eval(f"${name}.u"))


def _get_current_regs():
    """Save current GDB register values."""
    cfg = _ARCH_CONFIGS[_get_arch()]
    regs = {}
    for name in cfg["callee_saved"]:
        regs[name] = _read_reg_raw(name)
    return regs


def _set_regs(regs):
    """Set GDB register values from a dict.

    For aarch64 d-registers, set the '.u' (unsigned) member.
    """
    for name, val in regs.items():
        if name.startswith("d") and name[1:].isdigit():
            gdb.execute(f"set ${name}.u = {val:#x}", to_string=True)
        else:
            gdb.execute(f"set ${name} = {val:#x}", to_string=True)


def _find_fiber(arg):
    """Find a fiber by address (hex) or name. Returns (fiber_gdb_value, fiber_addr)."""
    fb_init = _get_fb_initializer()
    active_ptr = fb_init["active"]
    sched_ptr = fb_init["sched"]
    active_addr = int(active_ptr)

    # Try to parse as address first.
    try:
        target_addr = int(arg, 0)
        for fiber_ptr in _walk_fiber_list(sched_ptr):
            if int(fiber_ptr) == target_addr:
                return fiber_ptr.dereference(), int(fiber_ptr)
        raise gdb.GdbError(f"No fiber found at address {arg}")
    except ValueError:
        pass

    # Search by name.
    matches = []
    for fiber_ptr in _walk_fiber_list(sched_ptr):
        fiber_val = fiber_ptr.dereference()
        if _fiber_name(fiber_val) == arg:
            matches.append((fiber_val, int(fiber_ptr)))

    if len(matches) == 0:
        raise gdb.GdbError(f"No fiber named '{arg}' found in current thread")
    if len(matches) > 1:
        gdb.write(f"Warning: {len(matches)} fibers named '{arg}', using first match\n")
    return matches[0]


class FibersCommand(gdb.Command):
    """List all fibers in the current thread.

    Usage: fibers
    """

    def __init__(self):
        super().__init__("fibers", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        fb_init = _get_fb_initializer()
        active_ptr = fb_init["active"]
        sched_ptr = fb_init["sched"]
        active_addr = int(active_ptr)

        header = (
            f"  {'#':>3}  {'Address':<18} {'':2} {'Name':<23} "
            f"{'Type':<10} {'State':<12} {'Prio':<8} {'Stack':>6} {'Preempts':>10}"
        )
        gdb.write(header + "\n")
        gdb.write("-" * len(header) + "\n")

        idx = 0
        for fiber_ptr in _walk_fiber_list(sched_ptr):
            idx += 1
            fiber_val = fiber_ptr.dereference()
            addr = int(fiber_ptr)
            name = _fiber_name(fiber_val)
            ftype = TYPE_NAMES.get(int(fiber_val["type_"]), "?")
            state = _fiber_state(fiber_val, addr, active_addr)
            prio = PRIO_NAMES.get(int(fiber_val["prio_"]), "?")
            stack_kb = int(fiber_val["stack_size_"]) // 1024
            stack_str = f"{stack_kb}K" if stack_kb > 0 else "0"
            preempts = int(fiber_val["preempt_cnt_"])
            marker = "*" if addr == active_addr else " "

            gdb.write(
                f"  {idx:>3}  {addr:#018x} {marker:2} {name:<23} "
                f"{ftype:<10} {state:<12} {prio:<8} {stack_str:>6} {preempts:>10}\n"
            )

        if idx == 0:
            gdb.write("  No fibers found (fiber scheduler not initialized?)\n")
        else:
            gdb.write(f"\n  {idx} fiber(s) total\n")


class FiberBtCommand(gdb.Command):
    """Show backtrace of a specific fiber.

    Usage: fiber-bt <address|name>

    For the active fiber, this is equivalent to 'bt'.
    For suspended/ready/sleeping fibers, temporarily switches to
    the fiber's saved context, runs 'bt', then restores.
    """

    def __init__(self):
        super().__init__("fiber-bt", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        if not arg.strip():
            raise gdb.GdbError("Usage: fiber-bt <address|name>")

        fb_init = _get_fb_initializer()
        active_addr = int(fb_init["active"])

        fiber_val, fiber_addr = _find_fiber(arg.strip())
        name = _fiber_name(fiber_val)
        state = _fiber_state(fiber_val, fiber_addr, active_addr)

        gdb.write(f"Backtrace for fiber '{name}' ({state}) at {fiber_addr:#x}:\n")

        if fiber_addr == active_addr:
            gdb.execute("bt")
            return

        fctx = _read_fctx(fiber_addr)
        if fctx == 0:
            gdb.write("  <fiber has no saved context (entry_ is null)>\n")
            return

        saved_regs = _read_saved_regs(fctx)
        if saved_regs is None:
            gdb.write("  <cannot read saved registers>\n")
            return

        # Save current registers, switch, bt, restore.
        orig_regs = _get_current_regs()
        try:
            _set_regs(saved_regs)
            gdb.execute("bt")
        finally:
            _set_regs(orig_regs)


class FiberSwitchCommand(gdb.Command):
    """Switch GDB's register view to a fiber's saved context.

    Usage: fiber-switch <address|name>

    After switching, you can use 'bt', 'frame', 'info locals', etc.
    to inspect the fiber. Use 'fiber-restore' to switch back.
    """

    def __init__(self):
        super().__init__("fiber-switch", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        if not arg.strip():
            raise gdb.GdbError("Usage: fiber-switch <address|name>")

        thread_id = gdb.selected_thread().global_num

        if thread_id in _saved_contexts:
            raise gdb.GdbError(
                "Already switched to a fiber context in this thread. "
                "Use 'fiber-restore' first."
            )

        fb_init = _get_fb_initializer()
        active_addr = int(fb_init["active"])

        fiber_val, fiber_addr = _find_fiber(arg.strip())
        name = _fiber_name(fiber_val)

        if fiber_addr == active_addr:
            gdb.write(f"Fiber '{name}' is the active fiber, no switch needed.\n")
            return

        fctx = _read_fctx(fiber_addr)
        if fctx == 0:
            raise gdb.GdbError(f"Fiber '{name}' has no saved context (entry_ is null)")

        saved_regs = _read_saved_regs(fctx)
        if saved_regs is None:
            raise gdb.GdbError("Cannot read saved registers")

        orig_regs = _get_current_regs()
        _saved_contexts[thread_id] = orig_regs
        _set_regs(saved_regs)

        gdb.write(
            f"Switched to fiber '{name}' at {fiber_addr:#x}. "
            f"Use 'fiber-restore' to switch back.\n"
        )


class FiberRestoreCommand(gdb.Command):
    """Restore GDB's register view after a fiber-switch.

    Usage: fiber-restore
    """

    def __init__(self):
        super().__init__("fiber-restore", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        thread_id = gdb.selected_thread().global_num

        if thread_id not in _saved_contexts:
            raise gdb.GdbError("No saved context to restore for this thread.")

        orig_regs = _saved_contexts.pop(thread_id)
        _set_regs(orig_regs)
        gdb.write("Restored original register context.\n")


# Register all commands.
FibersCommand()
FiberBtCommand()
FiberSwitchCommand()
FiberRestoreCommand()

gdb.write(
    "Helio fiber debugging commands loaded (x86_64, aarch64): "
    "fibers, fiber-bt, fiber-switch, fiber-restore\n"
)
