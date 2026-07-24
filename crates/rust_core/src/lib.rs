pub mod network;
pub mod parsers;

use network::fast_proxy::FastProxyClient;
use parsers::binance_spot_sbe_parsers::parse_binance_spot_sbe_trades_rs;
use pyo3::prelude::*;

#[pymodule]
fn _rust_core(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FastProxyClient>()?;
    m.add_function(wrap_pyfunction!(parse_binance_spot_sbe_trades_rs, m)?)?;
    Ok(())
}
