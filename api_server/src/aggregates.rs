use crate::{
    time_range::{BucketsRange, FORMAT_STR_SECONDS},
    user_tag::Action,
};
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use std::fmt::{self, Display, Formatter};

#[derive(Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Deserialize)]
pub struct AggregatesQuery {
    pub time_range: BucketsRange,
    pub action: Action,
    pub origin: Option<String>,
    pub brand_id: Option<String>,
    pub category_id: Option<String>,
    aggregates: Vec<Aggregate>,
}

impl AggregatesQuery {
    pub fn aggregates(&self) -> &[Aggregate] {
        &self.aggregates
    }

    pub fn make_reply(self, rows: Vec<AggregatesRow>) -> anyhow::Result<AggregatesReply> {
        anyhow::ensure!(
            rows.len() == self.time_range.buckets_count(),
            "invalid rows count"
        );

        let expected_sum_price = self.aggregates.contains(&Aggregate::SumPrice);
        let expected_count = self.aggregates.contains(&Aggregate::Count);
        for row in &rows {
            anyhow::ensure!(
                !expected_sum_price || row.sum_price.is_some(),
                "row does not contain sum price"
            );
            anyhow::ensure!(
                !expected_count || row.count.is_some(),
                "row does not contain count"
            );
        }

        Ok(AggregatesReply { query: self, rows })
    }
}

pub struct AggregatesRow {
    pub sum_price: Option<usize>,
    pub count: Option<usize>,
}

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
                columns.push(aggr.to_string());
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
                            values.push(row.count.unwrap().to_string());
                        }
                        Aggregate::SumPrice => {
                            values.push(row.sum_price.unwrap().to_string());
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
