// The native library allocates through mimalloc rather than the platform allocator, so that it behaves the same everywhere. Arrow's buffers are large
// and short-lived: a column buffer per batch, freed when the batch is. The Windows heap serves blocks above ~512 KB with VirtualAlloc and returns them
// with VirtualFree, a kernel round trip with page faults and zeroed pages for every such buffer; glibc keeps and reuses them. mimalloc keeps and
// reuses them on every platform. Disable the `mimalloc` feature to fall back to the system allocator.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[macro_use]
mod macros;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/datafusion_sharp_proto.rs"));
}

pub mod cancellation;
pub mod common;
pub mod context;
pub mod dataframe;
pub mod error;
pub mod logger;
mod mappers;
pub mod memory_store;
pub mod runtime;

pub use common::*;
pub use error::*;
pub use proto::*;

pub use context::*;
pub use dataframe::*;
pub use runtime::*;
