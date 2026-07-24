use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use binance_sbe::ReadBuf;
use binance_sbe::message_header_codec::MessageHeaderDecoder;
use binance_sbe::trades_stream_event_codec::{TradesStreamEventDecoder, SBE_TEMPLATE_ID, SBE_SCHEMA_ID};

#[pyfunction]
pub fn parse_binance_spot_sbe_trades_rs(py: Python, payload: &[u8]) -> PyResult<PyObject> {
    if payload.len() < 8 {
        return Err(pyo3::exceptions::PyValueError::new_err("Payload too short"));
    }

    let buf = ReadBuf::new(payload);
    let mut header_decoder = MessageHeaderDecoder::default().wrap(buf, 0);

    let block_length = header_decoder.block_length();
    let template_id = header_decoder.template_id();
    let schema_id = header_decoder.schema_id();
    let version = header_decoder.version();

    if template_id != SBE_TEMPLATE_ID || schema_id != SBE_SCHEMA_ID {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unsupported template_id={} or schema_id={}", template_id, schema_id
        )));
    }

    let offset = 8; // SBE Header Length
    let mut trades_decoder = TradesStreamEventDecoder::default().wrap(
        buf,
        offset,
        block_length,
        version,
    );

    let event_time = trades_decoder.event_time();
    let transact_time = trades_decoder.transact_time();
    let price_exponent = trades_decoder.price_exponent();
    let qty_exponent = trades_decoder.qty_exponent();

    let py_list = PyList::empty_bound(py);

    let mut trades_group = trades_decoder.trades_decoder();
    while let Ok(Some(_)) = trades_group.advance() {
        let trade_id = trades_group.id();
        let price = trades_group.price();
        let qty = trades_group.qty();
        
        let is_buyer_maker = match trades_group.is_buyer_maker() {
            binance_sbe::bool_enum::BoolEnum::True => true,
            _ => false,
        };

        let py_dict = PyDict::new_bound(py);
        py_dict.set_item("event_time", event_time)?;
        py_dict.set_item("transact_time", transact_time)?;
        py_dict.set_item("price_exponent", price_exponent)?;
        py_dict.set_item("qty_exponent", qty_exponent)?;
        py_dict.set_item("id", trade_id)?;
        py_dict.set_item("price", price)?;
        py_dict.set_item("qty", qty)?;
        py_dict.set_item("is_buyer_maker", is_buyer_maker)?;

        py_list.append(py_dict)?;
    }

    Ok(py_list.into())
}
