//! Arrow schemas for the Parquet lake, RecordBatch builders per event type,
//! and Parquet serialization.
//!
//! Prices/quantities are stored as their exact decimal **strings** (Arrow
//! `Utf8`), lossless and matching the SpacetimeDB boundary. DataFusion can
//! `CAST(price AS DECIMAL(38, 8))` at query time. Each row also carries its
//! Kafka `partition`/`offset` for lineage. TODO: a native `Decimal128(38, 8)`
//! schema would let analytical queries aggregate without a cast.

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use common::events::{AggTrade, BookTicker, KlineEvent};
use parquet::arrow::ArrowWriter;

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
        field("price", DataType::Utf8),
        field("quantity", DataType::Utf8),
        field("trade_time", DataType::Int64),
        field("agg_trade_id", DataType::Int64),
        field("is_buyer_maker", DataType::Boolean),
    ];
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

pub fn build_trades(rows: &[&Row<AggTrade>]) -> Result<RecordBatch> {
    let mut symbol = StringBuilder::new();
    let mut price = StringBuilder::new();
    let mut quantity = StringBuilder::new();
    let mut trade_time = Int64Builder::new();
    let mut agg_trade_id = Int64Builder::new();
    let mut is_buyer_maker = BooleanBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();

    for r in rows {
        symbol.append_value(&r.event.symbol);
        price.append_value(r.event.price.to_string());
        quantity.append_value(r.event.quantity.to_string());
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
        field("best_bid_price", DataType::Utf8),
        field("best_bid_qty", DataType::Utf8),
        field("best_ask_price", DataType::Utf8),
        field("best_ask_qty", DataType::Utf8),
        field("update_id", DataType::Int64),
    ];
    fields.extend(lineage_fields());
    Arc::new(Schema::new(fields))
}

pub fn build_book_tickers(rows: &[&Row<BookTicker>]) -> Result<RecordBatch> {
    let mut symbol = StringBuilder::new();
    let mut best_bid_price = StringBuilder::new();
    let mut best_bid_qty = StringBuilder::new();
    let mut best_ask_price = StringBuilder::new();
    let mut best_ask_qty = StringBuilder::new();
    let mut update_id = Int64Builder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();

    for r in rows {
        symbol.append_value(&r.event.symbol);
        best_bid_price.append_value(r.event.best_bid_price.to_string());
        best_bid_qty.append_value(r.event.best_bid_qty.to_string());
        best_ask_price.append_value(r.event.best_ask_price.to_string());
        best_ask_qty.append_value(r.event.best_ask_qty.to_string());
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
        field("open", DataType::Utf8),
        field("high", DataType::Utf8),
        field("low", DataType::Utf8),
        field("close", DataType::Utf8),
        field("volume", DataType::Utf8),
        field("quote_volume", DataType::Utf8),
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
    let mut open = StringBuilder::new();
    let mut high = StringBuilder::new();
    let mut low = StringBuilder::new();
    let mut close = StringBuilder::new();
    let mut volume = StringBuilder::new();
    let mut quote_volume = StringBuilder::new();
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
        open.append_value(k.open.to_string());
        high.append_value(k.high.to_string());
        low.append_value(k.low.to_string());
        close.append_value(k.close.to_string());
        volume.append_value(k.volume.to_string());
        quote_volume.append_value(k.quote_volume.to_string());
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
}
