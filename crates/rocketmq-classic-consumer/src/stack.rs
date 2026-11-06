use std::{backtrace::Backtrace, thread};

pub(crate) fn dump_threads() -> String {
    #[cfg(all(
        target_os = "macos",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    {
        if let Ok(dump) = apple::dump() {
            return dump;
        }
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        if let Ok(dump) = linux::dump() {
            return dump;
        }
    }

    #[cfg(all(
        target_os = "windows",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        if let Ok(dump) = windows::dump() {
            return dump;
        }
    }

    current_thread_dump()
}

fn current_thread_dump() -> String {
    let current = thread::current();
    let name = current.name().unwrap_or("unnamed");
    format!(
        "\"{name}\" tid={:?} (current thread only on this target)\n{:?}",
        current.id(),
        Backtrace::force_capture()
    )
}

#[cfg(all(
    target_os = "macos",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
mod apple {
    use std::{
        ffi::{CStr, c_void},
        fmt::Write as _,
        mem::size_of,
        ptr, slice,
        sync::Mutex,
    };

    use mach2::{
        kern_return::KERN_SUCCESS,
        mach_port::mach_port_deallocate,
        mach_types::{thread_act_array_t, thread_act_t},
        message::mach_msg_type_number_t,
        task::task_threads,
        thread_act::{thread_get_state, thread_resume, thread_suspend},
        traps::mach_task_self,
        vm::{mach_vm_deallocate, mach_vm_read_overwrite},
        vm_types::{mach_vm_size_t, natural_t},
    };

    #[cfg(target_arch = "aarch64")]
    use mach2::{structs::arm_thread_state64_t, thread_status::ARM_THREAD_STATE64};
    #[cfg(target_arch = "x86_64")]
    use mach2::{structs::x86_thread_state64_t, thread_status::x86_THREAD_STATE64};

    const MAX_FRAMES: usize = 128;
    static CAPTURE_LOCK: Mutex<()> = Mutex::new(());

    struct RawStack {
        frames: [usize; MAX_FRAMES],
        len: usize,
    }

    impl RawStack {
        fn empty() -> Self {
            Self {
                frames: [0; MAX_FRAMES],
                len: 0,
            }
        }

        fn push(&mut self, address: u64) {
            if address != 0 && self.len < self.frames.len() {
                self.frames[self.len] = address as usize;
                self.len += 1;
            }
        }

        fn into_vec(self) -> Vec<usize> {
            self.frames[..self.len].to_vec()
        }
    }

    struct ThreadList {
        task: mach2::mach_types::task_t,
        ports: thread_act_array_t,
        count: mach_msg_type_number_t,
    }

    impl Drop for ThreadList {
        fn drop(&mut self) {
            if self.ports.is_null() {
                return;
            }
            unsafe {
                for port in slice::from_raw_parts(self.ports, self.count as usize) {
                    let _ = mach_port_deallocate(self.task, *port);
                }
                let size = (self.count as usize).saturating_mul(size_of::<thread_act_t>());
                let _ = mach_vm_deallocate(self.task, self.ports as u64, size as mach_vm_size_t);
            }
        }
    }

    struct ResumeThread(thread_act_t);

    impl Drop for ResumeThread {
        fn drop(&mut self) {
            unsafe {
                let _ = thread_resume(self.0);
            }
        }
    }

    struct ThreadDump {
        name: String,
        id: u64,
        current_backtrace: Option<String>,
        frames: Vec<usize>,
    }

    pub(super) fn dump() -> Result<String, &'static str> {
        let _capture_guard = CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = unsafe { mach_task_self() };
        let mut ports = ptr::null_mut();
        let mut count = 0;
        if unsafe { task_threads(task, &mut ports, &mut count) } != KERN_SUCCESS {
            return Err("task_threads failed");
        }
        let thread_list = ThreadList { task, ports, count };
        if thread_list.ports.is_null() {
            return Err("task_threads returned no thread list");
        }

        let current_port = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
        let mut dumps = Vec::with_capacity(thread_list.count as usize);
        for &port in unsafe { slice::from_raw_parts(thread_list.ports, thread_list.count as usize) }
        {
            let (name, id) = thread_name(port);
            if port == current_port {
                dumps.push(ThreadDump {
                    name,
                    id,
                    current_backtrace: Some(format!(
                        "{:?}",
                        std::backtrace::Backtrace::force_capture()
                    )),
                    frames: Vec::new(),
                });
                continue;
            }

            if unsafe { thread_suspend(port) } != KERN_SUCCESS {
                dumps.push(ThreadDump {
                    name,
                    id,
                    current_backtrace: None,
                    frames: Vec::new(),
                });
                continue;
            }
            let resume = ResumeThread(port);
            let raw_stack = unsafe { capture_frames(task, port) };
            drop(resume);
            let frames = raw_stack.into_vec();
            dumps.push(ThreadDump {
                name,
                id,
                current_backtrace: None,
                frames,
            });
        }

        let mut output = String::new();
        for dump in dumps {
            let _ = writeln!(output, "\"{}\" tid={}", dump.name, dump.id);
            if let Some(backtrace) = dump.current_backtrace {
                for line in backtrace.lines() {
                    let _ = writeln!(output, "    {line}");
                }
            } else if dump.frames.is_empty() {
                let _ = writeln!(output, "    <native stack unavailable>");
            } else {
                for symbol in symbolize(&dump.frames) {
                    let _ = writeln!(output, "    {symbol}");
                }
            }
        }
        Ok(output)
    }

    fn thread_name(port: thread_act_t) -> (String, u64) {
        let pthread = unsafe { libc::pthread_from_mach_thread_np(port) };
        if pthread == 0 {
            return (format!("thread-{port}"), u64::from(port));
        }

        let mut id = u64::from(port);
        unsafe {
            let _ = libc::pthread_threadid_np(pthread, &mut id);
        }
        let mut name = [0i8; 64];
        if unsafe { libc::pthread_getname_np(pthread, name.as_mut_ptr(), name.len()) } == 0 {
            let value = unsafe { CStr::from_ptr(name.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            if !value.is_empty() {
                return (value, id);
            }
        }
        (format!("thread-{id}"), id)
    }

    #[cfg(target_arch = "aarch64")]
    unsafe fn capture_frames(task: mach2::mach_types::task_t, port: thread_act_t) -> RawStack {
        let mut state = arm_thread_state64_t::new();
        let mut count = arm_thread_state64_t::count();
        let result = unsafe {
            thread_get_state(
                port,
                ARM_THREAD_STATE64,
                (&mut state as *mut arm_thread_state64_t).cast::<natural_t>(),
                &mut count,
            )
        };
        if result != KERN_SUCCESS {
            return RawStack::empty();
        }
        unsafe { walk_frames(task, state.__fp, state.__pc) }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn capture_frames(task: mach2::mach_types::task_t, port: thread_act_t) -> RawStack {
        let mut state = x86_thread_state64_t::new();
        let mut count = x86_thread_state64_t::count();
        let result = unsafe {
            thread_get_state(
                port,
                x86_THREAD_STATE64,
                (&mut state as *mut x86_thread_state64_t).cast::<natural_t>(),
                &mut count,
            )
        };
        if result != KERN_SUCCESS {
            return RawStack::empty();
        }
        unsafe { walk_frames(task, state.__rbp, state.__rip) }
    }

    unsafe fn walk_frames(
        task: mach2::mach_types::task_t,
        mut frame_pointer: u64,
        program_counter: u64,
    ) -> RawStack {
        let mut output = RawStack::empty();
        output.push(program_counter);
        while frame_pointer != 0 && output.len < MAX_FRAMES {
            let mut record = [0u64; 2];
            let mut copied = 0 as mach_vm_size_t;
            let result = unsafe {
                mach_vm_read_overwrite(
                    task,
                    frame_pointer,
                    size_of::<[u64; 2]>() as mach_vm_size_t,
                    record.as_mut_ptr() as u64,
                    &mut copied,
                )
            };
            if result != KERN_SUCCESS || copied != size_of::<[u64; 2]>() as mach_vm_size_t {
                break;
            }
            output.push(record[1]);
            let next_frame_pointer = record[0];
            if next_frame_pointer <= frame_pointer
                || next_frame_pointer.saturating_sub(frame_pointer) > 1024 * 1024
            {
                break;
            }
            frame_pointer = next_frame_pointer;
        }
        output
    }

    fn symbolize(frames: &[usize]) -> Vec<String> {
        let addresses = frames
            .iter()
            .map(|address| *address as *mut c_void)
            .collect::<Vec<_>>();
        let symbols =
            unsafe { libc::backtrace_symbols(addresses.as_ptr(), addresses.len() as libc::c_int) };
        if symbols.is_null() {
            return frames
                .iter()
                .map(|address| format!("0x{address:x}"))
                .collect();
        }
        let output = unsafe { slice::from_raw_parts(symbols, frames.len()) }
            .iter()
            .map(|symbol| {
                unsafe { CStr::from_ptr(*symbol) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        unsafe { libc::free(symbols.cast()) };
        output
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod linux {
    use std::{
        ffi::c_void,
        fs, io,
        mem::{self, size_of},
        ptr,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    const MAX_FRAMES: usize = 128;
    const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);
    const THREAD_SIGNAL_TIMEOUT: Duration = Duration::from_millis(100);
    // These signals default to ignore and can carry the targeted stack snapshot request.
    const SIGNAL_CANDIDATES: [libc::c_int; 4] =
        [libc::SIGWINCH, libc::SIGURG, libc::SIGCHLD, libc::SIGIO];
    const MAX_FRAME_GAP: u64 = 1024 * 1024;
    const MAX_STACK_SPAN: u64 = 32 * 1024 * 1024;

    pub(super) static CAPTURE_LOCK: Mutex<()> = Mutex::new(());
    static ACTIVE_TID: AtomicI32 = AtomicI32::new(0);
    static ACTIVE_SIGNAL: AtomicI32 = AtomicI32::new(0);
    static ACTIVE_TOKEN: AtomicUsize = AtomicUsize::new(0);
    static CAPTURED: AtomicBool = AtomicBool::new(false);
    static RELEASE: AtomicBool = AtomicBool::new(true);
    static NEXT_TOKEN: AtomicUsize = AtomicUsize::new(1);
    static FRAME_POINTER: AtomicU64 = AtomicU64::new(0);
    static STACK_POINTER: AtomicU64 = AtomicU64::new(0);
    static PROGRAM_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct ThreadInfo {
        id: libc::pid_t,
        name: String,
        blocked_signals: u64,
    }

    struct CapturedThread {
        id: libc::pid_t,
        name: String,
        frames: Vec<u64>,
        current_backtrace: Option<String>,
    }

    #[repr(C)]
    struct QueuedSignalInfo {
        signo: libc::c_int,
        errno: libc::c_int,
        code: libc::c_int,
        reserved: libc::c_int,
        pid: libc::pid_t,
        uid: libc::uid_t,
        value: libc::sigval,
        padding: [u8; 96],
    }

    const _: () = assert!(size_of::<QueuedSignalInfo>() == size_of::<libc::siginfo_t>());

    struct SignalHandler {
        signal: libc::c_int,
        previous: libc::sigaction,
    }

    impl Drop for SignalHandler {
        fn drop(&mut self) {
            let mut current = unsafe { mem::zeroed::<libc::sigaction>() };
            if unsafe { libc::sigaction(self.signal, ptr::null(), &mut current) } != 0
                || current.sa_sigaction != snapshot_signal_handler as usize
            {
                return;
            }
            unsafe {
                let _ = libc::sigaction(self.signal, &self.previous, ptr::null_mut());
            }
        }
    }

    pub(super) fn dump() -> Result<String, String> {
        let _capture_guard = CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        dump_locked()
    }

    pub(super) fn dump_locked() -> Result<String, String> {
        let threads = enumerate_threads().map_err(|error| error.to_string())?;
        if threads.is_empty() {
            return Err("/proc/self/task returned no threads".into());
        }
        let process_id = unsafe { libc::getpid() };
        let current_id = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };
        let deadline = Instant::now() + SNAPSHOT_TIMEOUT;
        let mut captured = Vec::with_capacity(threads.len());

        for thread_info in &threads {
            if thread_info.id == current_id {
                captured.push(CapturedThread {
                    id: thread_info.id,
                    name: thread_info.name.clone(),
                    frames: Vec::new(),
                    current_backtrace: Some(format!(
                        "{:?}",
                        std::backtrace::Backtrace::force_capture()
                    )),
                });
                continue;
            }

            let registers = capture_thread_with_available_signal(process_id, thread_info, deadline);
            let frames = registers
                .map(|(frame_pointer, stack_pointer, program_counter)| unsafe {
                    walk_frames(process_id, frame_pointer, stack_pointer, program_counter)
                })
                .unwrap_or_default();
            captured.push(CapturedThread {
                id: thread_info.id,
                name: thread_info.name.clone(),
                frames,
                current_backtrace: None,
            });
        }

        if captured
            .iter()
            .all(|thread| thread.frames.is_empty() && thread.current_backtrace.is_none())
        {
            return Err("Linux stack capture could not read any thread registers".into());
        }

        let mut dump = String::new();
        for thread in captured {
            use std::fmt::Write as _;
            let _ = writeln!(dump, "\"{}\" tid={}", thread.name, thread.id);
            if let Some(backtrace) = thread.current_backtrace {
                for line in backtrace.lines() {
                    let _ = writeln!(dump, "    {line}");
                }
            } else if thread.frames.is_empty() {
                let _ = writeln!(dump, "    <native stack unavailable>");
            } else {
                for frame in symbolize(&thread.frames) {
                    let _ = writeln!(dump, "    {frame}");
                }
            }
        }
        Ok(dump)
    }

    fn enumerate_threads() -> io::Result<Vec<ThreadInfo>> {
        let mut threads = Vec::new();
        for entry in fs::read_dir("/proc/self/task")? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if is_disappeared_task_error(&error) => continue,
                Err(error) => return Err(error),
            };
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|id| id.parse::<libc::pid_t>().ok())
            else {
                continue;
            };
            let status_path = entry.path().join("status");
            let blocked_signals = match read_blocked_signals(&status_path) {
                Ok(mask) => mask,
                Err(error) if is_disappeared_task_error(&error) => continue,
                Err(error) => return Err(error),
            };
            let name = fs::read_to_string(entry.path().join("comm"))
                .ok()
                .map(|name| name.trim_end().to_owned())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("thread-{id}"));
            threads.push(ThreadInfo {
                id,
                name,
                blocked_signals,
            });
        }
        threads.sort_by_key(|thread| thread.id);
        Ok(threads)
    }

    fn is_disappeared_task_error(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
    }

    fn read_blocked_signals(path: &std::path::Path) -> io::Result<u64> {
        let status = fs::read_to_string(path)?;
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix("SigBlk:").map(str::trim))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing SigBlk field"))?;
        u64::from_str_radix(value, 16)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn unblocked_signal_candidates(thread: &ThreadInfo) -> impl Iterator<Item = libc::c_int> + '_ {
        SIGNAL_CANDIDATES.iter().copied().filter(move |signal| {
            let mask = 1u64 << (*signal - 1);
            thread.blocked_signals & mask == 0
        })
    }

    #[cfg(test)]
    fn has_available_signal_candidate(thread: &ThreadInfo) -> bool {
        unblocked_signal_candidates(thread).any(|signal| {
            let mut previous = unsafe { mem::zeroed::<libc::sigaction>() };
            let result = unsafe { libc::sigaction(signal, ptr::null(), &mut previous) };
            result == 0
                && (previous.sa_sigaction == libc::SIG_DFL
                    || previous.sa_sigaction == libc::SIG_IGN)
        })
    }

    fn install_signal_handler(signal: libc::c_int) -> io::Result<SignalHandler> {
        let mut previous = unsafe { mem::zeroed::<libc::sigaction>() };
        if unsafe { libc::sigaction(signal, ptr::null(), &mut previous) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if previous.sa_sigaction != libc::SIG_DFL && previous.sa_sigaction != libc::SIG_IGN {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "signal already has a custom handler",
            ));
        }

        let mut action = unsafe { mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = snapshot_signal_handler as usize;
        action.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        if unsafe { libc::sigaction(signal, &action, ptr::null_mut()) } == 0 {
            Ok(SignalHandler { signal, previous })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn capture_thread_with_available_signal(
        process_id: libc::pid_t,
        thread: &ThreadInfo,
        deadline: Instant,
    ) -> Option<(u64, u64, u64)> {
        for signal in unblocked_signal_candidates(thread) {
            let Ok(signal_handler) = install_signal_handler(signal) else {
                continue;
            };
            let registers = capture_thread(process_id, thread.id, signal, deadline);
            drop(signal_handler);
            if registers.is_some() {
                return registers;
            }
        }
        None
    }

    #[cfg(test)]
    pub(super) fn named_thread_has_available_signal(name: &str) -> bool {
        enumerate_threads()
            .ok()
            .and_then(|threads| threads.into_iter().find(|thread| thread.name == name))
            .is_some_and(|thread| has_available_signal_candidate(&thread))
    }

    extern "C" fn snapshot_signal_handler(
        signal: libc::c_int,
        info: *mut libc::siginfo_t,
        context: *mut c_void,
    ) {
        if info.is_null()
            || signal != ACTIVE_SIGNAL.load(Ordering::Relaxed)
            || ACTIVE_TID.load(Ordering::Acquire)
                != unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
        {
            return;
        }
        let info = unsafe { &*info };
        let token = unsafe { info.si_value().sival_ptr as usize };
        if info.si_code != libc::SI_QUEUE
            || unsafe { info.si_pid() } != unsafe { libc::getpid() }
            || token != ACTIVE_TOKEN.load(Ordering::Acquire)
        {
            return;
        }
        let Some((frame_pointer, stack_pointer, program_counter)) =
            (unsafe { registers_from_context(context) })
        else {
            return;
        };
        FRAME_POINTER.store(frame_pointer, Ordering::Relaxed);
        STACK_POINTER.store(stack_pointer, Ordering::Relaxed);
        PROGRAM_COUNTER.store(program_counter, Ordering::Relaxed);
        CAPTURED.store(true, Ordering::Release);
        while !RELEASE.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn registers_from_context(context: *mut c_void) -> Option<(u64, u64, u64)> {
        let context = unsafe { &*context.cast::<libc::ucontext_t>() };
        Some((
            context.uc_mcontext.gregs[libc::REG_RBP as usize] as u64,
            context.uc_mcontext.gregs[libc::REG_RSP as usize] as u64,
            context.uc_mcontext.gregs[libc::REG_RIP as usize] as u64,
        ))
    }

    #[cfg(target_arch = "aarch64")]
    unsafe fn registers_from_context(context: *mut c_void) -> Option<(u64, u64, u64)> {
        let context = unsafe { &*context.cast::<libc::ucontext_t>() };
        Some((
            context.uc_mcontext.regs[29],
            context.uc_mcontext.sp,
            context.uc_mcontext.pc,
        ))
    }

    fn capture_thread(
        process_id: libc::pid_t,
        thread_id: libc::pid_t,
        signal: libc::c_int,
        deadline: Instant,
    ) -> Option<(u64, u64, u64)> {
        if Instant::now() >= deadline {
            return None;
        }
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed).max(1);
        CAPTURED.store(false, Ordering::Relaxed);
        RELEASE.store(false, Ordering::Relaxed);
        FRAME_POINTER.store(0, Ordering::Relaxed);
        STACK_POINTER.store(0, Ordering::Relaxed);
        PROGRAM_COUNTER.store(0, Ordering::Relaxed);
        ACTIVE_TOKEN.store(token, Ordering::Relaxed);
        ACTIVE_SIGNAL.store(signal, Ordering::Relaxed);
        ACTIVE_TID.store(thread_id, Ordering::Release);

        let mut info = QueuedSignalInfo {
            signo: signal,
            errno: 0,
            code: libc::SI_QUEUE,
            reserved: 0,
            pid: process_id,
            uid: unsafe { libc::getuid() },
            value: libc::sigval {
                sival_ptr: token as *mut c_void,
            },
            padding: [0; 96],
        };
        let queued = unsafe {
            libc::syscall(
                libc::SYS_rt_tgsigqueueinfo,
                process_id,
                thread_id,
                signal,
                (&mut info as *mut QueuedSignalInfo).cast::<c_void>(),
            )
        };
        if queued != 0 {
            RELEASE.store(true, Ordering::Release);
            ACTIVE_TID.store(0, Ordering::Release);
            return None;
        }

        let thread_deadline = (Instant::now() + THREAD_SIGNAL_TIMEOUT).min(deadline);
        while !CAPTURED.load(Ordering::Acquire) && Instant::now() < thread_deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let registers = CAPTURED.load(Ordering::Acquire).then(|| {
            (
                FRAME_POINTER.load(Ordering::Relaxed),
                STACK_POINTER.load(Ordering::Relaxed),
                PROGRAM_COUNTER.load(Ordering::Relaxed),
            )
        });
        RELEASE.store(true, Ordering::Release);
        ACTIVE_TID.store(0, Ordering::Release);
        registers
    }

    unsafe fn walk_frames(
        process_id: libc::pid_t,
        mut frame_pointer: u64,
        stack_pointer: u64,
        program_counter: u64,
    ) -> Vec<u64> {
        let mut frames = Vec::with_capacity(MAX_FRAMES);
        if program_counter != 0 {
            frames.push(program_counter);
        }
        if frame_pointer < stack_pointer
            || frame_pointer.saturating_sub(stack_pointer) > MAX_STACK_SPAN
            || frame_pointer & 7 != 0
        {
            return frames;
        }
        while frame_pointer != 0 && frames.len() < MAX_FRAMES {
            let mut record = [0u64; 2];
            let local = libc::iovec {
                iov_base: record.as_mut_ptr().cast::<c_void>(),
                iov_len: size_of::<[u64; 2]>(),
            };
            let remote = libc::iovec {
                iov_base: frame_pointer as usize as *mut c_void,
                iov_len: size_of::<[u64; 2]>(),
            };
            let read = unsafe { libc::process_vm_readv(process_id, &local, 1, &remote, 1, 0) };
            if read != size_of::<[u64; 2]>() as isize {
                break;
            }
            if record[1] != 0 {
                frames.push(record[1]);
            }
            let next = record[0];
            if next <= frame_pointer
                || next.saturating_sub(frame_pointer) > MAX_FRAME_GAP
                || next.saturating_sub(stack_pointer) > MAX_STACK_SPAN
            {
                break;
            }
            frame_pointer = next;
        }
        frames
    }

    fn symbolize(frames: &[u64]) -> Vec<String> {
        let addresses = frames
            .iter()
            .map(|address| *address as usize as *mut c_void)
            .collect::<Vec<_>>();
        let symbols =
            unsafe { libc::backtrace_symbols(addresses.as_ptr(), addresses.len() as libc::c_int) };
        if symbols.is_null() {
            return frames.iter().map(|frame| format!("0x{frame:x}")).collect();
        }
        let strings = unsafe { std::slice::from_raw_parts(symbols, frames.len()) }
            .iter()
            .map(|symbol| unsafe { std::ffi::CStr::from_ptr(*symbol) })
            .map(|symbol| symbol.to_string_lossy().into_owned())
            .collect();
        unsafe { libc::free(symbols.cast()) };
        strings
    }

    #[cfg(test)]
    mod tests {
        use super::{
            SIGNAL_CANDIDATES, ThreadInfo, is_disappeared_task_error, unblocked_signal_candidates,
        };

        #[test]
        fn task_disappearance_errors_are_recognized() {
            assert!(is_disappeared_task_error(&std::io::Error::from(
                std::io::ErrorKind::NotFound
            )));
            assert!(is_disappeared_task_error(
                &std::io::Error::from_raw_os_error(libc::ESRCH)
            ));
            assert!(!is_disappeared_task_error(&std::io::Error::from(
                std::io::ErrorKind::PermissionDenied
            )));
        }

        #[test]
        fn signal_candidate_falls_back_to_sigchld_when_common_signals_are_blocked() {
            let blocked_signals = (1u64 << (libc::SIGWINCH - 1)) | (1u64 << (libc::SIGURG - 1));
            let thread = ThreadInfo {
                id: 1,
                name: "test".into(),
                blocked_signals,
            };

            assert_eq!(
                unblocked_signal_candidates(&thread).collect::<Vec<_>>(),
                vec![libc::SIGCHLD, libc::SIGIO]
            );
        }

        #[test]
        fn signal_candidate_falls_back_to_sigio_when_other_candidates_are_blocked() {
            let blocked_signals = SIGNAL_CANDIDATES
                .iter()
                .filter(|signal| **signal != libc::SIGIO)
                .fold(0u64, |mask, signal| mask | (1u64 << (*signal - 1)));
            let thread = ThreadInfo {
                id: 1,
                name: "test".into(),
                blocked_signals,
            };

            assert_eq!(
                unblocked_signal_candidates(&thread).collect::<Vec<_>>(),
                vec![libc::SIGIO]
            );
        }

        #[test]
        fn signal_candidates_ignore_other_threads_block_masks() {
            let blocked_signals = SIGNAL_CANDIDATES
                .iter()
                .fold(0u64, |mask, signal| mask | (1u64 << (*signal - 1)));
            let target = ThreadInfo {
                id: 1,
                name: "target".into(),
                blocked_signals: 0,
            };
            let other = ThreadInfo {
                id: 2,
                name: "other".into(),
                blocked_signals,
            };

            assert_eq!(
                unblocked_signal_candidates(&target).collect::<Vec<_>>(),
                SIGNAL_CANDIDATES
            );
            assert_eq!(unblocked_signal_candidates(&other).count(), 0);
        }

        #[test]
        fn signal_candidate_selection_rejects_when_every_candidate_is_blocked() {
            let blocked_signals = SIGNAL_CANDIDATES
                .iter()
                .fold(0u64, |mask, signal| mask | (1u64 << (*signal - 1)));
            let thread = ThreadInfo {
                id: 1,
                name: "test".into(),
                blocked_signals,
            };

            assert_eq!(unblocked_signal_candidates(&thread).count(), 0);
        }
    }
}

#[cfg(all(
    target_os = "windows",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod windows {
    use std::{
        ffi::c_void,
        mem::size_of,
        ptr,
        sync::{Mutex, OnceLock},
        time::{Duration, Instant},
    };

    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
        System::{
            Diagnostics::{
                Debug::{
                    CONTEXT, GetThreadContext, RtlLookupFunctionEntry, RtlPcToFileHeader,
                    RtlVirtualUnwind, UNW_FLAG_NHANDLER, UNWIND_HISTORY_TABLE,
                },
                ToolHelp::{
                    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First,
                    Thread32Next,
                },
            },
            LibraryLoader::{GetModuleFileNameW, GetModuleHandleW, GetProcAddress},
            Threading::{
                GetCurrentProcess, GetCurrentProcessId, GetCurrentThreadId, OpenThread,
                ResumeThread, SuspendThread, THREAD_GET_CONTEXT, THREAD_QUERY_LIMITED_INFORMATION,
                THREAD_SUSPEND_RESUME,
            },
        },
    };

    #[cfg(target_arch = "x86_64")]
    use windows_sys::Win32::System::Diagnostics::Debug::{CONTEXT_FULL_AMD64, ReadProcessMemory};
    #[cfg(target_arch = "aarch64")]
    use windows_sys::Win32::System::Diagnostics::Debug::{
        CONTEXT_FULL_ARM64, IMAGE_ARM64_RUNTIME_FUNCTION_ENTRY,
    };

    const MAX_FRAMES: usize = 128;
    const MAX_THREAD_NAME_UNITS: usize = 4096;
    const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);
    const THREAD_CAPTURE_TIMEOUT: Duration = Duration::from_millis(100);
    const KERNEL32_WIDE: [u16; 13] = [
        b'k' as u16,
        b'e' as u16,
        b'r' as u16,
        b'n' as u16,
        b'e' as u16,
        b'l' as u16,
        b'3' as u16,
        b'2' as u16,
        b'.' as u16,
        b'd' as u16,
        b'l' as u16,
        b'l' as u16,
        0,
    ];
    static CAPTURE_LOCK: Mutex<()> = Mutex::new(());

    // windows-sys exposes CONTEXT without the native Windows alignment requirement.
    #[repr(C, align(16))]
    struct AlignedContext(CONTEXT);

    type GetThreadDescriptionFn = unsafe extern "system" fn(
        HANDLE,
        *mut windows_sys::core::PWSTR,
    ) -> windows_sys::core::HRESULT;

    static GET_THREAD_DESCRIPTION: OnceLock<Option<GetThreadDescriptionFn>> = OnceLock::new();

    struct Handle(HANDLE);

    impl Drop for Handle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                unsafe {
                    let _ = CloseHandle(self.0);
                }
            }
        }
    }

    struct SuspendedThread(HANDLE);

    impl SuspendedThread {
        fn new(handle: HANDLE) -> Option<Self> {
            (unsafe { SuspendThread(handle) } != u32::MAX).then_some(Self(handle))
        }
    }

    impl Drop for SuspendedThread {
        fn drop(&mut self) {
            unsafe {
                let _ = ResumeThread(self.0);
            }
        }
    }

    struct ThreadDump {
        id: u32,
        name: String,
        frames: Vec<u64>,
        current_backtrace: Option<String>,
    }

    pub(super) fn dump() -> Result<String, String> {
        let _capture_guard = CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let thread_ids = enumerate_threads().map_err(|error| error.to_string())?;
        if thread_ids.is_empty() {
            return Err("Toolhelp returned no process threads".into());
        }
        let process = unsafe { GetCurrentProcess() };
        let current_id = unsafe { GetCurrentThreadId() };
        let mut threads = thread_ids
            .into_iter()
            .map(|id| {
                let name = if id == current_id {
                    std::thread::current()
                        .name()
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("thread-{id}"))
                } else {
                    thread_name_by_id(id)
                };
                (id, name)
            })
            .collect::<Vec<_>>();
        threads.sort_by_key(|(id, name)| (*id != current_id, name == &format!("thread-{id}"), *id));
        let deadline = Instant::now() + SNAPSHOT_TIMEOUT;
        let mut dumps = Vec::with_capacity(threads.len());

        for (index, (id, name)) in threads.iter().enumerate() {
            if Instant::now() >= deadline {
                dumps.extend(threads[index..].iter().map(|(id, name)| ThreadDump {
                    id: *id,
                    name: name.clone(),
                    frames: Vec::new(),
                    current_backtrace: None,
                }));
                break;
            }
            if *id == current_id {
                dumps.push(ThreadDump {
                    id: *id,
                    name: name.clone(),
                    frames: Vec::new(),
                    current_backtrace: Some(format!(
                        "{:?}",
                        std::backtrace::Backtrace::force_capture()
                    )),
                });
                continue;
            }

            let handle = unsafe {
                OpenThread(
                    THREAD_GET_CONTEXT | THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
                    0,
                    *id,
                )
            };
            if handle.is_null() {
                dumps.push(ThreadDump {
                    id: *id,
                    name: name.clone(),
                    frames: Vec::new(),
                    current_backtrace: None,
                });
                continue;
            }
            let handle = Handle(handle);
            let thread_deadline = (Instant::now() + THREAD_CAPTURE_TIMEOUT).min(deadline);
            let frames = if let Some(_suspended) = SuspendedThread::new(handle.0) {
                unsafe { capture_stack(process, handle.0, thread_deadline) }
            } else {
                Vec::new()
            };
            dumps.push(ThreadDump {
                id: *id,
                name: name.clone(),
                frames,
                current_backtrace: None,
            });
        }

        if dumps
            .iter()
            .all(|dump| dump.frames.is_empty() && dump.current_backtrace.is_none())
        {
            return Err("Windows stack capture could not read any thread contexts".into());
        }

        let mut output = String::new();
        for dump in dumps {
            use std::fmt::Write as _;
            let _ = writeln!(output, "\"{}\" tid={}", dump.name, dump.id);
            if let Some(backtrace) = dump.current_backtrace {
                for line in backtrace.lines() {
                    let _ = writeln!(output, "    {line}");
                }
            } else if dump.frames.is_empty() {
                let _ = writeln!(output, "    <native stack unavailable>");
            } else {
                for frame in symbolize(&dump.frames) {
                    let _ = writeln!(output, "    {frame}");
                }
            }
        }
        Ok(output)
    }

    fn enumerate_threads() -> std::io::Result<Vec<u32>> {
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot.is_null() || snapshot == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error());
        }
        let _snapshot = Handle(snapshot);
        let process_id = unsafe { GetCurrentProcessId() };
        let mut entry = THREADENTRY32 {
            dwSize: size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        if unsafe { Thread32First(snapshot, &mut entry) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut threads = Vec::new();
        loop {
            if entry.th32OwnerProcessID == process_id {
                threads.push(entry.th32ThreadID);
            }
            if unsafe { Thread32Next(snapshot, &mut entry) } == 0 {
                break;
            }
        }
        threads.sort_unstable();
        Ok(threads)
    }

    fn thread_name(handle: HANDLE, id: u32) -> String {
        let Some(get_description) = get_thread_description_function() else {
            return format!("thread-{id}");
        };
        let mut description = ptr::null_mut();
        let result = unsafe { get_description(handle, &mut description) };
        if result < 0 || description.is_null() {
            return format!("thread-{id}");
        }
        let mut length = 0;
        while length < MAX_THREAD_NAME_UNITS && unsafe { *description.add(length) } != 0 {
            length += 1;
        }
        let name =
            String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(description, length) });
        unsafe {
            let _ = LocalFree(description.cast());
        }
        if name.is_empty() {
            format!("thread-{id}")
        } else {
            name
        }
    }

    fn thread_name_by_id(id: u32) -> String {
        let handle = unsafe { OpenThread(THREAD_QUERY_LIMITED_INFORMATION, 0, id) };
        if handle.is_null() {
            return format!("thread-{id}");
        }
        let handle = Handle(handle);
        thread_name(handle.0, id)
    }

    fn get_thread_description_function() -> Option<GetThreadDescriptionFn> {
        *GET_THREAD_DESCRIPTION.get_or_init(|| unsafe {
            let kernel32 = GetModuleHandleW(KERNEL32_WIDE.as_ptr());
            if kernel32.is_null() {
                return None;
            }
            let function = GetProcAddress(kernel32, c"GetThreadDescription".as_ptr().cast::<u8>());
            let function = function?;
            Some(std::mem::transmute::<
                unsafe extern "system" fn() -> isize,
                GetThreadDescriptionFn,
            >(function))
        })
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn capture_stack(process: HANDLE, thread: HANDLE, deadline: Instant) -> Vec<u64> {
        let mut context = AlignedContext(CONTEXT {
            ContextFlags: CONTEXT_FULL_AMD64,
            ..Default::default()
        });
        if unsafe { GetThreadContext(thread, &mut context.0) } == 0 {
            return Vec::new();
        }
        let context = &mut context.0;
        let mut frames = vec![context.Rip];
        let mut history = UNWIND_HISTORY_TABLE::default();
        while frames.len() < MAX_FRAMES && Instant::now() < deadline {
            let control_pc = context.Rip;
            if control_pc == 0 {
                break;
            }
            let mut image_base = 0u64;
            let function =
                unsafe { RtlLookupFunctionEntry(control_pc, &mut image_base, &mut history) };
            if function.is_null() {
                let mut return_address = 0u64;
                let mut bytes_read = 0usize;
                let read = unsafe {
                    ReadProcessMemory(
                        process,
                        context.Rsp as usize as *const c_void,
                        (&mut return_address as *mut u64).cast(),
                        size_of::<u64>(),
                        &mut bytes_read,
                    )
                };
                if read == 0 || bytes_read != size_of::<u64>() {
                    break;
                }
                context.Rsp = context.Rsp.saturating_add(size_of::<u64>() as u64);
                context.Rip = return_address;
            } else {
                let mut handler_data = ptr::null_mut();
                let mut establisher_frame = 0u64;
                unsafe {
                    let _ = RtlVirtualUnwind(
                        UNW_FLAG_NHANDLER,
                        image_base,
                        control_pc,
                        function,
                        context,
                        &mut handler_data,
                        &mut establisher_frame,
                        ptr::null_mut(),
                    );
                }
            }
            if context.Rip == 0 || context.Rip == control_pc {
                break;
            }
            frames.push(context.Rip);
        }
        frames.retain(|frame| *frame != 0);
        frames
    }

    #[cfg(target_arch = "aarch64")]
    unsafe fn capture_stack(_process: HANDLE, thread: HANDLE, deadline: Instant) -> Vec<u64> {
        let mut context = AlignedContext(CONTEXT::default());
        context.0.ContextFlags = CONTEXT_FULL_ARM64;
        if unsafe { GetThreadContext(thread, &mut context.0) } == 0 {
            return Vec::new();
        }
        let context = &mut context.0;
        let mut frames = vec![context.Pc];
        let mut history = UNWIND_HISTORY_TABLE::default();
        while frames.len() < MAX_FRAMES && Instant::now() < deadline {
            let control_pc = context.Pc;
            if control_pc == 0 {
                break;
            }
            let mut image_base = 0usize;
            let function = unsafe {
                RtlLookupFunctionEntry(control_pc as usize, &mut image_base, &mut history)
            };
            if function.is_null() {
                let return_address = unsafe { context.Anonymous.X[30] };
                if return_address == 0 {
                    break;
                }
                context.Pc = return_address;
            } else {
                let mut handler_data = ptr::null_mut();
                let mut establisher_frame = 0usize;
                unsafe {
                    let _ = RtlVirtualUnwind(
                        UNW_FLAG_NHANDLER,
                        image_base,
                        control_pc as usize,
                        function as *const IMAGE_ARM64_RUNTIME_FUNCTION_ENTRY,
                        context,
                        &mut handler_data,
                        &mut establisher_frame,
                        ptr::null_mut(),
                    );
                }
            }
            if context.Pc == 0 || context.Pc == control_pc {
                break;
            }
            frames.push(context.Pc);
        }
        frames
    }

    fn symbolize(frames: &[u64]) -> Vec<String> {
        frames.iter().map(|frame| format_frame(*frame)).collect()
    }

    fn format_frame(address: u64) -> String {
        let mut module_base = ptr::null_mut();
        if unsafe { RtlPcToFileHeader(address as usize as *const c_void, &mut module_base) }
            .is_null()
            || module_base.is_null()
        {
            return format!("0x{address:x}");
        }
        let mut module_path = [0u16; 1024];
        let path_length = unsafe {
            GetModuleFileNameW(
                module_base,
                module_path.as_mut_ptr(),
                module_path.len() as u32,
            )
        } as usize;
        if path_length == 0 || path_length >= module_path.len() {
            return format!("0x{address:x}");
        }
        let module_path = String::from_utf16_lossy(&module_path[..path_length]);
        let offset = address.saturating_sub(module_base as usize as u64);
        format!("{module_path}+0x{offset:x}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )))]
    use super::dump_threads;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };

    #[test]
    fn jstack_includes_current_and_other_named_threads() {
        let (started, ready) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let worker_cleanup = WorkerCleanup(release.clone());
        let worker_release = release.clone();
        let worker = std::thread::Builder::new()
            .name("jstack-worker".to_owned())
            .spawn(move || {
                started.send(()).unwrap();
                while !worker_release.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            })
            .unwrap();
        ready.recv().unwrap();

        #[cfg(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))]
        let (dump, worker_has_capture_signal) = dump_with_restored_signal_handler();
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )))]
        let dump = dump_threads();
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )))]
        let worker_has_capture_signal = true;
        #[cfg(target_os = "linux")]
        let (alternate_signal_dump, alternate_worker_has_capture_signal) =
            dump_with_custom_winch_handler();

        release.store(true, Ordering::Release);
        worker.join().unwrap();
        drop(worker_cleanup);

        assert!(dump.contains("jstack-worker"), "{dump}");
        assert!(dump.contains("tid="), "{dump}");
        assert!(dump.contains("Backtrace") || dump.contains("0x"), "{dump}");
        let lines = dump.lines().collect::<Vec<_>>();
        let worker_line = lines
            .iter()
            .position(|line| line.contains("jstack-worker"))
            .unwrap();
        let worker_has_stack = lines
            .get(worker_line + 1)
            .is_some_and(|line| !line.contains("native stack unavailable"));
        assert_eq!(worker_has_stack, worker_has_capture_signal, "{dump}");

        #[cfg(target_os = "linux")]
        {
            assert!(
                alternate_signal_dump.contains("jstack-worker"),
                "{alternate_signal_dump}"
            );
            let lines = alternate_signal_dump.lines().collect::<Vec<_>>();
            let worker_line = lines
                .iter()
                .position(|line| line.contains("jstack-worker"))
                .unwrap();
            let worker_has_stack = lines
                .get(worker_line + 1)
                .is_some_and(|line| !line.contains("native stack unavailable"));
            assert_eq!(
                worker_has_stack, alternate_worker_has_capture_signal,
                "{alternate_signal_dump}"
            );
        }
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn concurrent_jstack_captures_are_serialized_without_losing_threads() {
        let (started, ready) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let worker_release = release.clone();
        let worker = std::thread::Builder::new()
            .name("jstack-worker".to_owned())
            .spawn(move || {
                started.send(()).unwrap();
                while !worker_release.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            })
            .unwrap();
        ready.recv().unwrap();

        let worker_has_capture_signal =
            super::linux::named_thread_has_available_signal("jstack-worker");
        let capture_guard = super::linux::CAPTURE_LOCK.lock().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let spawn_capture = |barrier: Arc<std::sync::Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                super::linux::dump()
            })
        };
        let first = spawn_capture(barrier.clone());
        let second = spawn_capture(barrier.clone());
        barrier.wait();
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(capture_guard);

        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();
        release.store(true, Ordering::Release);
        worker.join().unwrap();

        for dump in [&first, &second] {
            assert!(dump.contains("jstack-worker"), "{dump}");
            let lines = dump.lines().collect::<Vec<_>>();
            let worker_line = lines
                .iter()
                .position(|line| line.contains("jstack-worker"))
                .unwrap();
            let worker_has_stack = lines
                .get(worker_line + 1)
                .is_some_and(|line| !line.contains("native stack unavailable"));
            assert_eq!(worker_has_stack, worker_has_capture_signal, "{dump}");
        }
    }

    #[cfg(target_os = "linux")]
    extern "C" fn custom_winch_handler(_signal: libc::c_int) {}

    #[cfg(target_os = "linux")]
    fn dump_with_restored_signal_handler() -> (String, bool) {
        let _capture_guard = super::linux::CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original = signal_handler(libc::SIGWINCH);
        let worker_has_capture_signal =
            super::linux::named_thread_has_available_signal("jstack-worker");
        let dump = super::linux::dump_locked()
            .unwrap_or_else(|error| panic!("Linux all-thread stack capture failed: {error}"));
        assert_eq!(signal_handler(libc::SIGWINCH), original);
        (dump, worker_has_capture_signal)
    }

    #[cfg(target_os = "linux")]
    fn signal_handler(signal: libc::c_int) -> usize {
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        assert_eq!(
            unsafe { libc::sigaction(signal, std::ptr::null(), &mut action) },
            0
        );
        action.sa_sigaction
    }

    #[cfg(target_os = "linux")]
    fn dump_with_custom_winch_handler() -> (String, bool) {
        let _capture_guard = super::linux::CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut custom_action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        custom_action.sa_sigaction = custom_winch_handler as usize;
        unsafe { libc::sigemptyset(&mut custom_action.sa_mask) };
        let mut previous = unsafe { std::mem::zeroed::<libc::sigaction>() };
        assert_eq!(
            unsafe { libc::sigaction(libc::SIGWINCH, &custom_action, &mut previous) },
            0
        );
        let _restore = RestoreSignalAction {
            signal: libc::SIGWINCH,
            previous,
        };

        let worker_has_capture_signal =
            super::linux::named_thread_has_available_signal("jstack-worker");
        let dump = super::linux::dump_locked()
            .unwrap_or_else(|error| panic!("Linux custom-signal stack capture failed: {error}"));
        let current = signal_handler(libc::SIGWINCH);
        assert_eq!(current, custom_action.sa_sigaction);
        (dump, worker_has_capture_signal)
    }

    struct WorkerCleanup(Arc<AtomicBool>);

    impl Drop for WorkerCleanup {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[cfg(target_os = "linux")]
    struct RestoreSignalAction {
        signal: libc::c_int,
        previous: libc::sigaction,
    }

    #[cfg(target_os = "linux")]
    impl Drop for RestoreSignalAction {
        fn drop(&mut self) {
            unsafe {
                let _ = libc::sigaction(self.signal, &self.previous, std::ptr::null_mut());
            }
        }
    }
}
