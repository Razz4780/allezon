use api_server::user_tag::UserTag;
use serde::{ser::SerializeMap, Serialize};

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ListOrder {
    Ordered,
    Unordered,
}

pub enum DbOp {
    ListAppend {
        bin_name: String,
        value: (i64, UserTag),
    },
    ListTrim {
        bin_name: String,
        index: usize,
        count: usize,
    },
    ListSetOrder {
        bin_name: String,
        list_order: ListOrder,
    },
    Add {
        bin_name: String,
        incr: i64,
    },
}

impl Serialize for DbOp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DbOp::ListAppend { bin_name, value } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "LIST_APPEND")?;
                map.serialize_entry("binName", bin_name)?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            DbOp::ListTrim {
                bin_name,
                index,
                count,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("type", "LIST_TRIM")?;
                map.serialize_entry("binName", bin_name)?;
                map.serialize_entry("index", index)?;
                map.serialize_entry("count", count)?;
                map.end()
            }
            DbOp::ListSetOrder {
                bin_name,
                list_order,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "LIST_SET_ORDER")?;
                map.serialize_entry("binName", bin_name)?;
                map.serialize_entry("listOrder", list_order)?;
                map.end()
            }
            DbOp::Add { bin_name, incr } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "ADD")?;
                map.serialize_entry("binName", bin_name)?;
                map.serialize_entry("incr", incr)?;
                map.end()
            }
        }
    }
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct OperateDbRequest {
    opsList: Vec<DbOp>,
}

impl OperateDbRequest {
    pub fn update_user_profile(tag: UserTag) -> Self {
        let value = (-tag.time.timestamp_millis(), tag);
        let append_op = DbOp::ListAppend {
            bin_name: "user_tags".into(),
            value,
        };
        let trim_op = DbOp::ListTrim {
            bin_name: "user_tags".into(),
            index: 0,
            count: 200,
        };

        Self {
            opsList: vec![append_op, trim_op],
        }
    }

    pub fn list_set_order() -> Self {
        let op = DbOp::ListSetOrder {
            bin_name: "user_tags".into(),
            list_order: ListOrder::Ordered,
        };

        Self { opsList: vec![op] }
    }

    pub fn update_aggregate_record(tag: &UserTag) -> Self {
        let count_add = DbOp::Add {
            bin_name: "count".into(),
            incr: 1,
        };
        let sum_price_add = DbOp::Add {
            bin_name: "sum_price".into(),
            incr: tag.product_info.price.into(),
        };

        Self {
            opsList: vec![count_add, sum_price_add],
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub enum IndexType {
    Numeric,
    String,
    Geo2DSphere,
}

pub struct CreateIndexRequest {
    pub type_: IndexType,
    pub name: String,
    pub namespace: String,
    pub set: String,
    pub bin: String,
}

impl Serialize for CreateIndexRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("type", &self.type_)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("namespace", &self.namespace)?;
        map.serialize_entry("set", &self.set)?;
        map.serialize_entry("bin", &self.bin)?;
        map.end()
    }
}
