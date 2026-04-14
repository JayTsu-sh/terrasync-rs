pub mod api;
pub mod application;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod server;

pub use server::start_web_server;

pub mod prelude {
    pub use crate::error::{Result, WebError};
    pub use crate::server::start_web_server;
}
