//! Local times as Python's time.strftime writes them: log lines ("%Y-%m-%dT%H:%M:%S") and the broadcast hook's
//! ("%Y-%m-%d %H:%M:%S").

/// secs (Unix time) in local time, the date and the time joined by sep.
pub fn local(secs: i64, sep: char) -> String {
    let t = secs as libc::time_t;
    let mut tm = std::mem::MaybeUninit::<libc::tm>::zeroed();
    // SAFETY: localtime_r only writes the tm it is given, which lives for the call; zeroed is a valid tm.
    let tm = unsafe {
        libc::localtime_r(&t, tm.as_mut_ptr());
        tm.assume_init()
    };
    format!(
        "{}-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}

/// Now, for log lines.
pub fn stamp() -> String {
    local(crate::json::now() as i64, 'T')
}
