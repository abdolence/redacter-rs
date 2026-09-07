//! European national identifiers, split by sub-region: Western and Southern Europe,
//! the Nordic and Baltic countries, and Central/Eastern/South-Eastern Europe.

mod eastern;
mod nordic;
mod western;

pub use eastern::*;
pub use nordic::*;
pub use western::*;
