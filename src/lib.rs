#![feature(
    async_closure,
    adt_const_params,
    const_trait_impl,
    const_convert,
    const_option,
    derive_default_enum,
    generators,
    generator_trait,
    let_else,
    poll_ready,
    step_trait,
    stmt_expr_attributes,
    try_blocks,
    slice_pattern,
    never_type,
    core_intrinsics,
    inline_const,
    nll
)]
#![recursion_limit = "256"]
#![allow(
    dead_code,
    incomplete_features,
    clippy::mutable_key_type,
    clippy::type_complexity,
    clippy::unused_io_amount
)]

pub mod accessors;
#[doc(hidden)]
pub mod binutil;
mod bitmapdb;
pub mod chain;
pub mod consensus;
pub mod crypto;
pub mod etl;
pub mod execution;
pub mod kv;
pub mod models;
pub mod p2p;
pub mod res;
pub mod rpc;
pub mod sentry;
pub mod stagedsync;
pub mod stages;
mod state;
pub mod trie;
pub(crate) mod util;

pub use stagedsync::stages::StageId;
pub use state::*;
pub use util::*;
