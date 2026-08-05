//! Storya shared protocol types.

pub mod service {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/generated/rust/storya.service.rs"
    ));
}

pub mod transport {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/generated/rust/storya.transport.rs"
    ));
}
