use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{self, Display, Formatter};

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Device {
    Pc,
    Mobile,
    Tv,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
    View,
    Buy,
}

impl Display for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::View => f.write_str("VIEW"),
            Self::Buy => f.write_str("BUY"),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ProductInfo {
    pub product_id: i32,
    pub brand_id: String,
    pub category_id: String,
    pub price: i32,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct UserTag {
    #[serde(serialize_with = "serialize_datetime")]
    pub time: DateTime<Utc>,
    pub cookie: String,
    pub country: String,
    pub device: Device,
    pub action: Action,
    pub origin: String,
    pub product_info: ProductInfo,
}

fn serialize_datetime<S: Serializer>(
    datetime: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let as_string = datetime.to_rfc3339_opts(SecondsFormat::Millis, true);
    serializer.serialize_str(&as_string)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Serializer;

    #[test]
    fn ser_de_datetime() {
        let as_str = "\"2022-03-22T12:15:00.000Z\"";
        let expected = Utc.with_ymd_and_hms(2022, 3, 22, 12, 15, 0).unwrap();

        let deserialized: DateTime<Utc> = serde_json::from_str(as_str).unwrap();
        assert_eq!(deserialized, expected);

        let mut buffer = vec![];
        let mut serializer = Serializer::new(&mut buffer);
        serialize_datetime(&expected, &mut serializer).unwrap();
        let serialized = String::from_utf8(buffer).unwrap();
        assert_eq!(serialized, as_str);
    }
}
