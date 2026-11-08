//! Generates standard Tonic services using a build-local, pinned protoc binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        prost,
        &["proto/raddle/v1/dispatch.proto"],
        &["proto"],
    )?;
    println!("cargo:rerun-if-changed=proto/raddle/v1/dispatch.proto");
    Ok(())
}
