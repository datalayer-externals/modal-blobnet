use std::error::Error;

use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    // Instruct cargo to re-run this build script for new git commits
    EmitBuilder::builder().all_build().all_git().emit()?;

    // Generate version information for use in `blobnet --version`.
    shadow_rs::new()?;

    // Build protobufs
    prost_build::compile_protos(&["src/api.proto"], &["src/"])?;

    Ok(())
}
