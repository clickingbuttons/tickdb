use chrono::{Datelike, NaiveDateTime};

pub trait ToNaiveDateTime {
  fn to_naive_date_time(self) -> NaiveDateTime;
}

impl ToNaiveDateTime for i64 {
  fn to_naive_date_time(self) -> NaiveDateTime {
    let seconds = self / 1_000_000_000;
    let nanoseconds = self % 1_000_000_000;
    NaiveDateTime::from_timestamp(seconds, nanoseconds as u32)
  }
}

pub const NANOS_IN_SEC: i64 = 1_000_000_000;

const SECOND_FMTS: [&str; 4] = [
 "%S", // Second (00-61)	02
 "%X", // Time representation *	14:55:02
 "%T", // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S	14:55:02
 "%r", // 12-hour clock time *	02:55:02 pm
];

const MINUTE_FMTS: [&str; 3] = [
 "%M", // Minute (00-59)	55
 "%R", // 24-hour HH:MM time, equivalent to %H:%M	14:55
 "%c", // Date and time representation *	Thu Aug 23 14:55:02 2001
];

const HOUR_FMTS: [&str; 2] = [
 "%H", // Hour in 24h format (00-23)	14
 "%I", // Hour in 12h format (01-12)	02
];

const DAY_FMTS: [&str; 10] = [
 "%j", // Day of the year (001-366)	235
 "%d", // Day of the month, zero-padded (01-31)	23
 "%e", // Day of the month, space-padded ( 1-31)	23
 "%x", // Date representation *	08/23/01
 "%a", // Abbreviated weekday name *	Thu
 "%A", // Full weekday name *	Thursday
 "%u", // ISO 8601 weekday as number with Monday as 1 (1-7)	4
 "%w", // Weekday as a decimal number with Sunday as 0 (0-6)	4
 "%D", // Short MM/DD/YY date, equivalent to %m/%d/%y	08/23/01
 "%F", // Short YYYY-MM-DD date, equivalent to %Y-%m-%d	2001-08-23
];

const WEEK_FMTS: [&str; 3] = [
 "%V", // ISO 8601 week number (01-53)	34
 "%U", // Week number with the first Sunday as the first day of week one
	   // (00-53)	33
 "%W", // Week number with the first Monday as the first day of week one
	   // (00-53)	34
];

const MONTH_FMTS: [&str; 4] = [
 "%b", // Abbreviated month name *	Aug
 "%h", // Abbreviated month name * (same as %b)	Aug
 "%B", // Full month name *	August
 "%m", // Month as a decimal number (01-12)	08
];

const YEAR_FMTS: [&str; 5] = [
 "%C", // Year divided by 100 and truncated to integer (00-99)	20
 "%g", // Week-based year, last two digits (00-99)	01
 "%G", // Week-based year	2001
 "%y", // Year, last two digits (00-99)	01
 "%Y", // Year	2001
];

const DAYS_IN_MONTH: [i64; 12] = [
	31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
];

fn is_leap(year: i32) -> bool {
	if year % 400 == 0 {
		return true;
	}
	else if year % 100 == 0 {
		return false;
	}
	else if year % 4 == 0 {
		return true;
	}
	return false;
}

fn highest_res_specifier(fmt: &str, time: NaiveDateTime) -> i64 {
	for f in SECOND_FMTS {
		if fmt.contains(f) {
			return NANOS_IN_SEC;
		}
	}

	for f in MINUTE_FMTS {
		if fmt.contains(f) {
			return 60 * NANOS_IN_SEC;
		}
	}

	for f in HOUR_FMTS {
		if fmt.contains(f) {
			return 60 * 60 * NANOS_IN_SEC;
		}
	}

	if fmt.contains("%p") {
		return 12 * 60 * 60 * NANOS_IN_SEC;
	}

	for f in DAY_FMTS {
		if fmt.contains(f) {
			return 24 * 60 * 60 * NANOS_IN_SEC;
		}
	}

	for f in WEEK_FMTS {
		if fmt.contains(f) {
			return 7 * 24 * 60 * 60 * NANOS_IN_SEC;
		}
	}

	for f in MONTH_FMTS {
		if fmt.contains(f) {
			let mut days = DAYS_IN_MONTH[(time.month() - 1) as usize];
			if time.month() == 2 && is_leap(time.year()) {
				days += 1;
			}
			return days * 24 * 60 * 60 * NANOS_IN_SEC;
		}
	}

	for f in YEAR_FMTS {
		if fmt.contains(f) {
			let mut days = 365;
			if is_leap(time.year()) {
				days += 1;
			}
			return days * 24 * 60 * 60 * NANOS_IN_SEC;
		}
	}

	0
}

pub fn partition_min_ts(partition_fmt: &str, time: NaiveDateTime) -> i64 {
	let increment = highest_res_specifier(partition_fmt, time);
	let nanos = time.timestamp_nanos();
	nanos - nanos % increment
}

pub fn partition_max_ts(partition_fmt: &str, time: NaiveDateTime) -> i64 {
	let increment = highest_res_specifier(partition_fmt, time);
	let nanos = time.timestamp_nanos();
	(nanos / increment + 1) * increment - 1
}

