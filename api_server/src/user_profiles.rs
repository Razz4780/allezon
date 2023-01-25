use crate::user_tag::UserTag;
use chrono::{DateTime, Utc};
use serde::{
    de::{self, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::fmt::{self, Formatter};

#[derive(Deserialize)]
pub struct UserProfilesQuery {
    pub time_range: TimeRange,
    pub limit: Option<u32>,
}

#[derive(Debug, PartialEq, Eq)]

pub struct TimeRange {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

impl TimeRange {
    pub fn from(&self) -> &DateTime<Utc> {
        &self.from
    }

    pub fn to(&self) -> &DateTime<Utc> {
        &self.to
    }
}

impl<'de> Deserialize<'de> for TimeRange {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimeRangeVisitor;

        impl<'de> Visitor<'de> for TimeRangeVisitor {
            type Value = TimeRange;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a time range string in format \"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000\"")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                // TODO this does not work without the 'Z' suffix :))

                let make_err = || E::invalid_value(Unexpected::Str(v), &self);

                let mut chunks = v.split('_');

                let v = chunks.next().ok_or_else(make_err)?;
                let from: DateTime<Utc> = DateTime::parse_from_rfc3339(v)
                    .map_err(|_| make_err())?
                    .with_timezone(&Utc);
                let v = chunks.next().ok_or_else(make_err)?;
                let to: DateTime<Utc> = DateTime::parse_from_rfc3339(v)
                    .map_err(|_| make_err())?
                    .with_timezone(&Utc);

                if chunks.next().is_some() || from > to {
                    return Err(make_err());
                }

                Ok(Self::Value { from, to })
            }
        }

        deserializer.deserialize_str(TimeRangeVisitor)
    }
}

#[derive(Serialize)]
pub struct UserProfilesReply {
    pub cookie: String,
    pub views: Vec<UserTag>,
    pub buys: Vec<UserTag>,
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn ser_de_timerange() {
        let expected = TimeRange {
            from: Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 00).unwrap(),
            to: Utc.with_ymd_and_hms(2022, 3, 22, 12, 30, 00).unwrap(),
        };

        let as_str = "\"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000\"";
        let deserialized: TimeRange = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        let as_str = "\"2022-03-22T12:15:00_2022-03-22T12:30:00\"";
        let deserialized: TimeRange = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        let as_str = "\"2022-03-22T12:30:00.000_2022-03-22T12:15:00.000\"";
        serde_json::from_str::<TimeRange>(as_str).unwrap_err();

        let as_str = "\"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000_2022-03-22T12:45:00.000\"";
        serde_json::from_str::<TimeRange>(as_str).unwrap_err();
    }
}
