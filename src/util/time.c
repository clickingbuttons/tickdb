#include "time.h"

struct tm nanos_to_tm(i64 nanos) {
	struct tm res;
	nanos /= NANOS_IN_SEC;
	// TODO: localtime config flag
	memcpy(&res, gmtime(&nanos), sizeof(struct tm));
	return res;
}

static char* second_fmts[] = {
 "%S", // Second (00-61)	02
 "%X", // Time representation *	14:55:02
 "%T", // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S	14:55:02
 "%r", // 12-hour clock time *	02:55:02 pm
};

static char* minute_fmts[] = {
 "%M", // Minute (00-59)	55
 "%R", // 24-hour HH:MM time, equivalent to %H:%M	14:55
 "%c", // Date and time representation *	Thu Aug 23 14:55:02 2001
};

static char* hour_fmts[] = {
 "%H", // Hour in 24h format (00-23)	14
 "%I", // Hour in 12h format (01-12)	02
};

static char* day_fmts[] = {
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
};

static char* week_fmts[] = {
 "%V", // ISO 8601 week number (01-53)	34
 "%U", // Week number with the first Sunday as the first day of week one
	   // (00-53)	33
 "%W", // Week number with the first Monday as the first day of week one
	   // (00-53)	34
};

static char* month_fmts[] = {
 "%b", // Abbreviated month name *	Aug
 "%h", // Abbreviated month name * (same as %b)	Aug
 "%B", // Full month name *	August
 "%m", // Month as a decimal number (01-12)	08
};

static char* year_fmts[] = {
 "%C", // Year divided by 100 and truncated to integer (00-99)	20
 "%g", // Week-based year, last two digits (00-99)	01
 "%G", // Week-based year	2001
 "%y", // Year, last two digits (00-99)	01
 "%Y", // Year	2001
};

static int days_in_month[] = {
 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
};

static bool is_leap(i32 year) {
	if (year % 400 == 0)
		return true;
	else if (year % 100 == 0)
		return false;
	else if (year % 4 == 0)
		return true;
	return false;
}

i64 min_format_specifier(string* partition_fmt, struct tm* time) {
	char* haystack = string_data(partition_fmt);
	for (int i = 0; i < sizeof(second_fmts) / sizeof(second_fmts[0]); i++)
		if (strstr(haystack, second_fmts[i]) != NULL)
			return NANOS_IN_SEC;

	for (int i = 0; i < sizeof(minute_fmts) / sizeof(minute_fmts[0]); i++)
		if (strstr(haystack, minute_fmts[i]) != NULL)
			return 60 * NANOS_IN_SEC;

	for (int i = 0; i < sizeof(hour_fmts) / sizeof(hour_fmts[0]); i++)
		if (strstr(haystack, hour_fmts[i]) != NULL)
			return 60 * 60 * NANOS_IN_SEC;

	if (strstr(haystack, "%p") != NULL) // AM or PM designation
		return 60 * 60 * 12 * NANOS_IN_SEC;

	for (int i = 0; i < sizeof(day_fmts) / sizeof(day_fmts[0]); i++)
		if (strstr(haystack, day_fmts[i]) != NULL)
			return 60 * 60 * 24 * NANOS_IN_SEC;

	for (int i = 0; i < sizeof(month_fmts) / sizeof(month_fmts[0]); i++) {
		if (strstr(haystack, month_fmts[i]) != NULL) {
			u64 days = days_in_month[time->tm_mon];
			if (time->tm_mon == 1 && is_leap(time->tm_year))
				days += 1;
			return 60 * 60 * 24 * days * NANOS_IN_SEC;
		}
	}

	// If these are the highest resolution partition format the user is doing
	// something wrong...
	// %z	ISO 8601 offset from UTC in timezone (1 minute=1, 1 hour=100) If
	// timezone cannot be determined, no characters	+100 %Z	Timezone name or
	// abbreviation * If timezone cannot be determined, no characters	CDT
	for (int i = 0; i < sizeof(year_fmts) / sizeof(year_fmts[0]); i++) {
		if (strstr(haystack, year_fmts[i]) != NULL) {
			u64 days = 365;
			if (is_leap(time->tm_year))
				days += 1;
			return 60 * 60 * 24 * days * NANOS_IN_SEC;
		}
	}

	return 0;
}

i64 min_partition_ts(string* partition_fmt, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
	i64 increment = min_format_specifier(partition_fmt, &time);
	return epoch_nanos - epoch_nanos % increment;
}

i64 max_partition_ts(string* partition_fmt, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
	i64 increment = min_format_specifier(partition_fmt, &time);
	return (epoch_nanos / increment + 1) * increment;
}
