use std::{
    fmt::Write as _,
    os::unix::net::UnixDatagram,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

const SYSLOG_PATH: &str = "/dev/log";
const SD_ID: &str = "bnw@55029";
const FACILITY_USER: u64 = 1;

pub struct SyslogLayer {
    socket: UnixDatagram,
    hostname: String,
    app_name: String,
    pid: u32,
}

impl SyslogLayer {
    pub fn try_new() -> Option<Self> {
        let socket = UnixDatagram::unbound().ok()?;
        socket.connect(SYSLOG_PATH).ok()?;

        let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
            .unwrap_or_default()
            .trim()
            .to_owned();
        let hostname = if hostname.is_empty() {
            "-".to_owned()
        } else {
            hostname
        };

        Some(Self {
            socket,
            hostname,
            app_name: env!("CARGO_PKG_NAME").replace('-', "_"),
            pid: std::process::id(),
        })
    }
}

impl<S: Subscriber> Layer<S> for SyslogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let severity = level_to_severity(*event.metadata().level());
        let pri = FACILITY_USER * 8 + severity as u64;
        let ts = format_timestamp();

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let sd = build_sd(&visitor.fields);

        let msg = format!(
            "<{pri}>1 {ts} {hostname} {app} {pid} - {sd} {message}\n",
            hostname = self.hostname,
            app = self.app_name,
            pid = self.pid,
            message = visitor.message,
        );

        let _ = self.socket.send(msg.as_bytes());
    }
}

fn level_to_severity(level: tracing::Level) -> u8 {
    match level {
        tracing::Level::ERROR => 3,
        tracing::Level::WARN => 4,
        tracing::Level::INFO => 6,
        tracing::Level::DEBUG | tracing::Level::TRACE => 7,
    }
}

fn build_sd(fields: &[(&'static str, String)]) -> String {
    if fields.is_empty() {
        return "-".to_owned();
    }
    let mut s = format!("[{SD_ID}");
    for (k, v) in fields {
        write!(s, " {}=\"{}\"", k, escape_sd(v)).unwrap();
    }
    s.push(']');
    s
}

fn escape_sd(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            ']' => out.push_str("\\]"),
            c => out.push(c),
        }
    }
    out
}

/// Format current UTC time as RFC 3339 with millisecond precision.
/// Uses only std — no chrono dep needed.
fn format_timestamp() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let millis = d.subsec_millis();

    let sec = secs % 60;
    let min = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;
    let days = secs / 86400;

    // Civil-date algorithm: http://howardhinnant.github.io/date_algorithms.html
    let z = days as i64 + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let mon = if mp < 10 { mp + 3 } else { mp - 9 };
    let yr = yoe as i64 + era * 400 + if mon <= 2 { 1 } else { 0 };

    format!("{yr:04}-{mon:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: Vec<(&'static str, String)>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields.push((field.name(), value.to_owned()));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        self.record_str(field, &s);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.push((field.name(), value.to_string()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.push((field.name(), value.to_string()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.push((field.name(), value.to_string()));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields.push((field.name(), value.to_string()));
    }
}
