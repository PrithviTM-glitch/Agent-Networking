//! Log line timestamps, local time as Python's time.strftime("%Y-%m-%dT%H:%M:%S") writes them.

pub fn stamp() -> String {
    let t =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs()) as libc::time_t;
    let mut tm = std::mem::MaybeUninit::<libc::tm>::zeroed();
    // SAFETY: localtime_r only writes the tm it is given, which lives for the call; zeroed is a valid tm.
    let tm = unsafe {
        libc::localtime_r(&t, tm.as_mut_ptr());
        tm.assume_init()
    };
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}
