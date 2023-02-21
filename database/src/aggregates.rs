use crate::{
    time_range::{BucketsRange, FORMAT_STR_SECONDS},
    user_tag::Action,
};
use chrono::{DateTime, Utc};
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::fmt::{self, Display, Formatter};

#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Aggregate {
    Count,
    SumPrice,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => f.write_str("COUNT"),
            Self::SumPrice => f.write_str("SUM_PRICE"),
        }
    }
}

impl Aggregate {
    pub fn db_name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::SumPrice => "sum_price",
        }
    }
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AggregatesQuery {
    pub time_range: BucketsRange,
    pub action: Action,
    pub origin: Option<String>,
    pub brand_id: Option<String>,
    pub category_id: Option<String>,
    pub aggregates: Vec<Aggregate>,
}

impl AggregatesQuery {
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Option<Self> {
        let mut time_range = None;
        let mut action = None;
        let mut origin = None;
        let mut brand_id = None;
        let mut category_id = None;
        let mut aggregates = Vec::new();

        let pairs = pairs.into_iter().map(|(k, v)| (k, Value::String(v)));

        for (key, value) in pairs {
            match key.as_str() {
                "time_range" if time_range.is_none() => {
                    time_range.replace(serde_json::from_value(value).ok()?);
                }
                "action" if action.is_none() => {
                    action.replace(serde_json::from_value(value).ok()?);
                }
                "origin" if origin.is_none() => {
                    origin.replace(serde_json::from_value(value).ok()?);
                }
                "brand_id" if brand_id.is_none() => {
                    brand_id.replace(serde_json::from_value(value).ok()?);
                }
                "category_id" if category_id.is_none() => {
                    category_id.replace(serde_json::from_value(value).ok()?);
                }
                "aggregates" if aggregates.len() < 2 => {
                    let aggregate = serde_json::from_value(value).ok()?;
                    if aggregates.contains(&aggregate) {
                        return None;
                    }
                    aggregates.push(aggregate);
                }
                _ => {
                    return None;
                }
            }
        }

        if aggregates.is_empty() {
            return None;
        }

        let time_range = time_range?;
        let action = action?;

        Some(Self {
            time_range,
            action,
            origin,
            brand_id,
            category_id,
            aggregates,
        })
    }

    pub fn aggregates(&self) -> &[Aggregate] {
        &self.aggregates
    }

    pub fn make_reply(self, rows: Vec<AggregatesRow>) -> anyhow::Result<AggregatesReply> {
        anyhow::ensure!(
            rows.len() == self.time_range.buckets_count(),
            "invalid rows count"
        );

        Ok(AggregatesReply { query: self, rows })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AggregatesRow {
    pub sum_price: usize,
    pub count: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AggregatesReply {
    query: AggregatesQuery,
    rows: Vec<AggregatesRow>,
}

impl Serialize for AggregatesReply {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut root = serializer.serialize_struct("AggregatesReply", 2)?;

        let columns = {
            let mut columns: Vec<String> = Vec::with_capacity(5 + self.query.aggregates.len());

            columns.push("1m_bucket".into());
            columns.push("action".into());
            if self.query.origin.is_some() {
                columns.push("origin".into());
            }
            if self.query.brand_id.is_some() {
                columns.push("brand_id".into());
            }
            if self.query.category_id.is_some() {
                columns.push("category_id".into());
            }
            for aggr in &self.query.aggregates {
                let aggr_str = match aggr {
                    Aggregate::Count => "count",
                    Aggregate::SumPrice => "sum_price",
                }
                .into();
                columns.push(aggr_str);
            }

            columns
        };
        root.serialize_field("columns", &columns)?;

        let rows = {
            let mut rows: Vec<Vec<String>> = Vec::with_capacity(self.rows.len());

            for (row, bucket) in self.rows.iter().zip(self.query.time_range.bucket_starts()) {
                let mut values: Vec<String> = Vec::with_capacity(columns.len());

                values.push(bucket.format(FORMAT_STR_SECONDS).to_string());
                values.push(self.query.action.to_string());
                if let Some(origin) = self.query.origin.as_ref() {
                    values.push(origin.clone());
                }
                if let Some(brand_id) = self.query.brand_id.as_ref() {
                    values.push(brand_id.clone());
                }
                if let Some(category_id) = self.query.category_id.as_ref() {
                    values.push(category_id.clone());
                }
                for aggr in &self.query.aggregates {
                    match aggr {
                        Aggregate::Count => {
                            values.push(row.count.to_string());
                        }
                        Aggregate::SumPrice => {
                            values.push(row.sum_price.to_string());
                        }
                    }
                }

                rows.push(values)
            }

            rows
        };
        root.serialize_field("rows", &rows)?;

        root.end()
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct AggregatesBucket {
    timestamp: i64,
    origin: Option<String>,
    brand_id: Option<String>,
    category_id: Option<String>,
}

impl AggregatesBucket {
    pub fn new(
        time: DateTime<Utc>,
        origin: Option<String>,
        brand_id: Option<String>,
        category_id: Option<String>,
    ) -> Self {
        Self {
            timestamp: time.timestamp() / 60,
            origin,
            brand_id,
            category_id,
        }
    }
}

impl Display for AggregatesBucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}--{}--{}--{}",
            self.timestamp,
            self.origin.as_deref().unwrap_or(""),
            self.brand_id.as_deref().unwrap_or(""),
            self.category_id.as_deref().unwrap_or(""),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn make_reply() {
        let time_range: BucketsRange =
            serde_json::from_str("\"2022-03-22T12:15:00_2022-03-22T12:17:00\"").unwrap();
        let query = AggregatesQuery {
            time_range,
            action: Action::Buy,
            origin: None,
            brand_id: None,
            category_id: None,
            aggregates: vec![Aggregate::Count],
        };

        query
            .clone()
            .make_reply(vec![
                AggregatesRow {
                    sum_price: 2,
                    count: 1,
                },
                AggregatesRow {
                    sum_price: 2,
                    count: 2,
                },
            ])
            .unwrap();

        // Invalid row count.
        query
            .make_reply(vec![AggregatesRow {
                sum_price: 1,
                count: 1,
            }])
            .unwrap_err();
    }
}
