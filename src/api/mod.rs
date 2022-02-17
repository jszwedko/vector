mod handler;
mod schema;
mod server;
mod tap;

pub(self) use schema::build_schema;
pub use server::Server;
use tokio::sync::oneshot;

// Shutdown channel types used by the server and tap.
type ShutdownTx = oneshot::Sender<()>;
type ShutdownRx = oneshot::Receiver<()>;
