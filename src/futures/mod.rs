#[macro_use]
mod r#trait;
pub use r#trait::IoFuture;
pub mod read;
pub use read::ReadFuture;
pub mod socket;
pub use socket::SocketFuture;
