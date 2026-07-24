//! Arrow schemas for the Parquet lake, RecordBatch builders per event type,
//! and Parquet serialization.
//!
//! Prices/quantities are stored as native `Decimal128(38, 8)` so DataFusion can
//! aggregate them (`AVG`, `SUM`, …) without a per-query cast. Scale 8 is
//! lossless for Binance payloads, which never carry more than 8 fractional
//! digits; precision 38 leaves 30 integer digits, far more than any quote
//! volume needs. The api casts the column back to a string at the JSON boundary
//! so the wire format stays exact-decimal-as-string (see `batches_to_json`).
//! Each row also carries its Kafka `partition`/`offset` for lineage.

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use common::events::{AggTrade, BookTicker, KlineEvent};
use parquet::arrow::ArrowWriter;
use rust_decimal::Decimal;

/// Lake decimal precision/scale. See the module docs for the rationale.
const DECIMAL_PRECISION: u8 = 38;
const DECIMAL_SCALE: i8 = 8;

/// A buffered event tagged with its Kafka coordinates (for deterministic file
/// naming and offset commits).
pub struct Row<T> {
    pub partition: i32,
    pub offset: i64,
    pub event: T,
}

fn field(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, false)
}

/// A `Decimal128(38, 8)` field.
fn decimal_field(name: &str) -> Field {
    field(name, DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE))
}

/// A `Decimal128Builder` typed to the lake's precision/scale, so its finished
/// array matches the schema field exactly.
fn decimal_builder() -> Decimal128Builder {
    Decimal128Builder::new().with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE))
}

/// Convert a `rust_decimal::Decimal` to the lake's `Decimal128(38, 8)`
/// coefficient: an `i128` scaled to 8 fractional digits.
///
/// Binance never sends more than 8 fractional digits, so `rescale(8)` only ever
/// pads with zeros here. If a value ever had more, it rounds to 8 places (the
/// same rounding a `CAST(... AS DECIMAL(38, 8))` would have applied at query
/// time) rather than corrupting the row. The rescaled coefficient is at most
/// ~29 digits — well inside both `i128` and precision 38.
fn to_decimal128(value: Decimal) -> i128 {
    let mut value = value;
    value.rescale(DECIMAL_SCALE as u32);
    value.mantissa()
}

fn lineage_fields() -> [Field; 2] {
    [
        field("kafka_partition", DataType::Int32),
        field("kafka_offset", DataType::Int64),
    ]
}

/// Serialize a RecordBatch to in-memory Parquet bytes.
pub fn to_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

// --- trades ---

pub fn trades_schema() -> SchemaRef {
    let mut fields = vec![
        field("symbol", DataType::Utf8),
        decimal_field("price"),
        decimal_field("quantity"),
        field("trade_time", DataType::Int64),
        field("agg_trade_id", DataType::Int64),
        field("is_buyer_maker", DataType::Boolean),
    ];
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

pub fn build_trades(rows: &[&Row<AggTrade>]) -> Result<RecordBatch> {
    let mut symbol = StringBuilder::new();
    let mut price = decimal_builder();
    let mut quantity = decimal_builder();
    let mut trade_time = Int64Builder::new();
    let mut agg_trade_id = Int64Builder::new();
    let mut is_buyer_maker = BooleanBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();

    for r in rows {
        symbol.append_value(&r.event.symbol);
        price.append_value(to_decimal128(r.event.price));
        quantity.append_value(to_decimal128(r.event.quantity));
        trade_time.append_value(r.event.trade_time);
        agg_trade_id.append_value(r.event.agg_trade_id);
        is_buyer_maker.append_value(r.event.is_buyer_maker);
        partition.append_value(r.partition);
        offset.append_value(r.offset);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbol.finish()),
        Arc::new(price.finish()),
        Arc::new(quantity.finish()),
        Arc::new(trade_time.finish()),
        Arc::new(agg_trade_id.finish()),
        Arc::new(is_buyer_maker.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
    ];
    Ok(RecordBatch::try_new(trades_schema(), columns)?)
}

// --- book tickers ---

pub fn book_tickers_schema() -> SchemaRef {
    let mut fields = vec![
        field("symbol", DataType::Utf8),
        decimal_field("best_bid_price"),
        decimal_field("best_bid_qty"),
        decimal_field("best_ask_price"),
        decimal_field("best_ask_qty"),
        field("update_id", DataType::Int64),
    ];
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

pub fn build_book_tickers(rows: &[&Row<BookTicker>]) -> Result<RecordBatch> {
    let mut symbol = StringBuilder::new();
    let mut best_bid_price = decimal_builder();
    let mut best_bid_qty = decimal_builder();
    let mut best_ask_price = decimal_builder();
    let mut best_ask_qty = decimal_builder();
    let mut update_id = Int64Builder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();

    for r in rows {
        symbol.append_value(&r.event.symbol);
        best_bid_price.append_value(to_decimal128(r.event.best_bid_price));
        best_bid_qty.append_value(to_decimal128(r.event.best_bid_qty));
        best_ask_price.append_value(to_decimal128(r.event.best_ask_price));
        best_ask_qty.append_value(to_decimal128(r.event.best_ask_qty));
        update_id.append_value(r.event.update_id);
        partition.append_value(r.partition);
        offset.append_value(r.offset);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbol.finish()),
        Arc::new(best_bid_price.finish()),
        Arc::new(best_bid_qty.finish()),
        Arc::new(best_ask_price.finish()),
        Arc::new(best_ask_qty.finish()),
        Arc::new(update_id.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
    ];
    Ok(RecordBatch::try_new(book_tickers_schema(), columns)?)
}

// --- klines ---

pub fn klines_schema() -> SchemaRef {
    let mut fields = vec![
        field("symbol", DataType::Utf8),
        field("interval", DataType::Utf8),
        decimal_field("open"),
        decimal_field("high"),
        decimal_field("low"),
        decimal_field("close"),
        decimal_field("volume"),
        decimal_field("quote_volume"),
        field("trade_count", DataType::Int64),
        field("open_time", DataType::Int64),
        field("close_time", DataType::Int64),
        field("is_closed", DataType::Boolean),
    ];
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

pub fn build_klines(rows: &[&Row<KlineEvent>]) -> Result<RecordBatch> {
    let mut symbol = StringBuilder::new();
    let mut interval = StringBuilder::new();
    let mut open = decimal_builder();
    let mut high = decimal_builder();
    let mut low = decimal_builder();
    let mut close = decimal_builder();
    let mut volume = decimal_builder();
    let mut quote_volume = decimal_builder();
    let mut trade_count = Int64Builder::new();
    let mut open_time = Int64Builder::new();
    let mut close_time = Int64Builder::new();
    let mut is_closed = BooleanBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();

    for r in rows {
        let k = &r.event.kline;
        symbol.append_value(&k.symbol);
        interval.append_value(&k.interval);
        open.append_value(to_decimal128(k.open));
        high.append_value(to_decimal128(k.high));
        low.append_value(to_decimal128(k.low));
        close.append_value(to_decimal128(k.close));
        volume.append_value(to_decimal128(k.volume));
        quote_volume.append_value(to_decimal128(k.quote_volume));
        trade_count.append_value(k.trade_count);
        open_time.append_value(k.open_time);
        close_time.append_value(k.close_time);
        is_closed.append_value(k.is_closed);
        partition.append_value(r.partition);
        offset.append_value(r.offset);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbol.finish()),
        Arc::new(interval.finish()),
        Arc::new(open.finish()),
        Arc::new(high.finish()),
        Arc::new(low.finish()),
        Arc::new(close.finish()),
        Arc::new(volume.finish()),
        Arc::new(quote_volume.finish()),
        Arc::new(trade_count.finish()),
        Arc::new(open_time.finish()),
        Arc::new(close_time.finish()),
        Arc::new(is_closed.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
    ];
    Ok(RecordBatch::try_new(klines_schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn trade(offset: i64) -> Row<AggTrade> {
        Row {
            partition: 0,
            offset,
            event: AggTrade {
                event_type: "aggTrade".into(),
                event_time: 1,
                symbol: "BTCUSDT".into(),
                agg_trade_id: 1,
                price: Decimal::from_str("64000.5").unwrap(),
                quantity: Decimal::from_str("0.1").unwrap(),
                first_trade_id: 1,
                last_trade_id: 1,
                trade_time: 2,
                is_buyer_maker: true,
            },
        }
    }

    #[test]
    fn builds_trades_batch_and_serializes_parquet() {
        let rows = [trade(10), trade(11)];
        let refs: Vec<&Row<AggTrade>> = rows.iter().collect();
        let batch = build_trades(&refs).expect("batch builds");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8);

        let bytes = to_parquet(&batch).expect("parquet serializes");
        // Parquet files start with the "PAR1" magic and are non-trivial.
        assert_eq!(&bytes[..4], b"PAR1");
        assert!(bytes.len() > 100);
    }

    #[test]
    fn to_decimal128_scales_to_eight_places() {
        // Fewer than 8 fractional digits: pad with zeros, exactly.
        assert_eq!(
            to_decimal128(Decimal::from_str("64000.5").unwrap()),
            6_400_050_000_000
        );
        assert_eq!(to_decimal128(Decimal::from_str("0.1").unwrap()), 10_000_000);
        // Exactly 8 places: the coefficient is the value with the point removed.
        assert_eq!(to_decimal128(Decimal::from_str("0.00000001").unwrap()), 1);
        // More than 8 places rounds to 8 rather than corrupting the row.
        assert_eq!(to_decimal128(Decimal::from_str("0.000000015").unwrap()), 2);
        assert_eq!(to_decimal128(Decimal::ZERO), 0);
    }

    #[test]
    fn price_columns_are_decimal128() {
        let batch = build_trades(&[&trade(1)]).expect("batch builds");
        let schema = batch.schema();
        for name in ["price", "quantity"] {
            assert_eq!(
                schema.field_with_name(name).unwrap().data_type(),
                &DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                "{name} should be Decimal128"
            );
        }
        // The stored coefficient round-trips the input value at scale 8.
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(price.value(0), 6_400_050_000_000);
    }
}
