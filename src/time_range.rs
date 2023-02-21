use chrono::{DateTime, Duration, NaiveDateTime, Timelike, Utc};
use serde::{
    de::{self, Unexpected, Visitor},
    Deserialize, Deserializer,
};
use std::fmt::{self, Formatter};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]

pub struct TimeRange<const BUCKETS: bool> {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

impl<const BUCKETS: bool> TimeRange<BUCKETS> {
    pub fn from(&self) -> &DateTime<Utc> {
        &self.from
    }

    pub fn to(&self) -> &DateTime<Utc> {
        &self.to
    }
}

pub type SimpleTimeRange = TimeRange<false>;

pub type BucketsRange = TimeRange<true>;

impl BucketsRange {
    pub fn buckets_count(&self) -> usize {
        (self.to - self.from).num_minutes().try_into().unwrap()
    }

    pub fn bucket_starts(&self) -> impl '_ + Iterator<Item = DateTime<Utc>> {
        let count = i64::try_from(self.buckets_count()).unwrap();
        (0..count).map(|idx| self.from + Duration::minutes(idx))
    }
}

struct TimeRangeVisitor<const BUCKETS: bool>;

const FORMAT_STR_MILLIS: &str = "%Y-%m-%dT%H:%M:%S%.3f";
pub const FORMAT_STR_SECONDS: &str = "%Y-%m-%dT%H:%M:%S";

impl<'de, const BUCKETS: bool> Visitor<'de> for TimeRangeVisitor<BUCKETS> {
    type Value = TimeRange<BUCKETS>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result where {
        let msg = if BUCKETS {
            "a 1-minute bucket range string in format \"2022-03-22T12:15:00_2022-03-22T12:30:00\", maximum 10 minutes"
        } else {
            "a time range string in format \"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000\""
        };

        f.write_str(msg)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        let make_err = || E::invalid_value(Unexpected::Str(v), &self);
        let format_str = if BUCKETS {
            FORMAT_STR_SECONDS
        } else {
            FORMAT_STR_MILLIS
        };

        let mut chunks = v.split('_');

        let v = chunks.next().ok_or_else(make_err)?;
        let from: NaiveDateTime =
            NaiveDateTime::parse_from_str(v, format_str).map_err(|_| make_err())?;
        let v = chunks.next().ok_or_else(make_err)?;
        let to: NaiveDateTime =
            NaiveDateTime::parse_from_str(v, format_str).map_err(|_| make_err())?;

        if chunks.next().is_some() || from > to {
            return Err(make_err());
        }

        if BUCKETS
            && (from.second() != 0 || to.second() != 0 || (to - from) > Duration::minutes(10))
        {
            return Err(make_err());
        }

        Ok(Self::Value {
            from: DateTime::from_utc(from, Utc),
            to: DateTime::from_utc(to, Utc),
        })
    }
}

impl<'de, const BUCKETS: bool> Deserialize<'de> for TimeRange<BUCKETS> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(TimeRangeVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parse_datetime() {
        let expected = Utc
            .with_ymd_and_hms(2022, 1, 12, 3, 45, 12)
            .unwrap()
            .with_nanosecond(1000000)
            .unwrap();
        let as_str = "2022-01-12T03:45:12.001";
        let parsed: DateTime<Utc> = DateTime::from_utc(
            NaiveDateTime::parse_from_str(as_str, FORMAT_STR_MILLIS).unwrap(),
            Utc,
        );
        assert_eq!(expected, parsed);

        let expected = Utc.with_ymd_and_hms(2022, 1, 12, 3, 45, 12).unwrap();
        let as_str = "2022-01-12T03:45:12";
        let parsed: DateTime<Utc> = DateTime::from_utc(
            NaiveDateTime::parse_from_str(as_str, FORMAT_STR_SECONDS).unwrap(),
            Utc,
        );
        assert_eq!(expected, parsed);
    }

    #[test]
    fn de_simpletimerange() {
        let expected = SimpleTimeRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 30, 0).unwrap(),
        };
        let as_str = "\"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000\"";
        let deserialized: SimpleTimeRange = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        let expected = SimpleTimeRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 12).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 30, 1).unwrap(),
        };
        let as_str = "\"2022-03-22T12:15:12.000_2022-03-22T12:30:01.000\"";
        let deserialized: SimpleTimeRange = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        // End earlier than begin.
        let as_str = "\"2022-03-22T12:30:00.000_2022-03-22T12:15:00.000\"";
        serde_json::from_str::<SimpleTimeRange>(as_str).unwrap_err();

        // More than 2 datetimes.
        let as_str = "\"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000_2022-03-22T12:45:00.000\"";
        serde_json::from_str::<SimpleTimeRange>(as_str).unwrap_err();
    }

    #[test]
    fn de_bucketsrange() {
        let expected = BucketsRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 25, 0).unwrap(),
        };

        let as_str = "\"2022-03-22T12:15:00_2022-03-22T12:25:00\"";
        let deserialized: BucketsRange = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        // Milliseconds precision.
        let as_str = "\"2022-03-22T12:15:00.000_2022-03-22T12:25:00.000\"";
        serde_json::from_str::<BucketsRange>(as_str).unwrap_err();

        // Not full minutes.
        let as_str = "\"2022-03-22T12:20:01_2022-03-22T12:22:00\"";
        serde_json::from_str::<BucketsRange>(as_str).unwrap_err();

        // End earlier than begin.
        let as_str = "\"2022-03-22T12:30:00.001_2022-03-22T12:25:00\"";
        serde_json::from_str::<BucketsRange>(as_str).unwrap_err();

        // More than 2 datetimes.
        let as_str = "\"2022-03-22T12:15:00_2022-03-22T12:30:00_2022-03-22T12:45:00\"";
        serde_json::from_str::<BucketsRange>(as_str).unwrap_err();

        // More than 10 minutes.
        let as_str = "\"2022-03-22T12:20:00_2022-03-22T12:31:00\"";
        serde_json::from_str::<BucketsRange>(as_str).unwrap_err();
    }

    #[test]
    fn buckets() {
        let range = BucketsRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 20, 0).unwrap(),
        };

        assert_eq!(range.buckets_count(), 5);

        let starts = range
            .bucket_starts()
            .map(|s| s.format(FORMAT_STR_SECONDS).to_string())
            .collect::<Vec<_>>();
        let expected = vec![
            "2022-03-22T12:15:00".to_string(),
            "2022-03-22T12:16:00".to_string(),
            "2022-03-22T12:17:00".to_string(),
            "2022-03-22T12:18:00".to_string(),
            "2022-03-22T12:19:00".to_string(),
        ];
        assert_eq!(starts, expected);

        let range = BucketsRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap(),
        };

        assert_eq!(range.buckets_count(), 0);

        let starts = range
            .bucket_starts()
            .map(|s| s.format(FORMAT_STR_SECONDS).to_string())
            .collect::<Vec<_>>();
        let expected: Vec<String> = Default::default();
        assert_eq!(starts, expected);
    }
}
