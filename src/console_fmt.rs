use std::fmt;

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{
    fmt::{
        format::Writer,
        time::{FormatTime, SystemTime},
        FmtContext, FormatEvent, FormatFields,
    },
    registry::LookupSpan,
};

/// Event formatter: `TIMESTAMP LEVEL target: [k=v k=v] message`
#[derive(Default)]
pub struct PrefixedFields {
    timer: SystemTime,
}

impl<S, N> FormatEvent<S, N> for PrefixedFields
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let ansi = writer.has_ansi_escapes();

        self.timer.format_time(&mut writer)?;
        write!(writer, " ")?;

        if ansi {
            write!(writer, "\x1b[{}m{:>5}\x1b[0m ", level_color(*meta.level()), meta.level())?;
        } else {
            write!(writer, "{:>5} ", meta.level())?;
        }

        write!(writer, "{}: ", meta.target())?;

        let mut visitor = Visitor::default();
        event.record(&mut visitor);

        if !visitor.fields.is_empty() {
            write!(writer, "[")?;
            for (i, (k, v)) in visitor.fields.iter().enumerate() {
                if i > 0 { write!(writer, " ")?; }
                write!(writer, "{k}={v}")?;
            }
            write!(writer, "] ")?;
        }

        writeln!(writer, "{}", visitor.message)
    }
}

fn level_color(level: Level) -> &'static str {
    match level {
        Level::ERROR => "31",
        Level::WARN  => "33",
        Level::INFO  => "32",
        Level::DEBUG => "34",
        Level::TRACE => "36",
    }
}

#[derive(Default)]
struct Visitor {
    message: String,
    fields: Vec<(&'static str, String)>,
}

impl tracing::field::Visit for Visitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields.push((field.name(), value.to_owned()));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
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
