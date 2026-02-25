fn main() -> Result<(), Box<dyn std::error::Error>> {
    generate_proto()
}

fn generate_proto() -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use std::path::PathBuf;

    // Collect all .proto files in ../proto directory (excluding vendor subdirectory)
    let proto_dir = PathBuf::from("../proto");
    let mut proto_files = Vec::new();
    for entry in fs::read_dir(&proto_dir)? {
        let path = entry?.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("proto") {
            proto_files.push(path.to_string_lossy().to_string());
        }
    }

    // Compile the proto files
    let mut cfg = prost_build::Config::new();

    cfg
        .extern_path(".datafusion_common", "::datafusion_proto::protobuf")
        .extern_path(".datafusion", "::datafusion_proto::protobuf");

    cfg.compile_protos(
        &proto_files,
        &["../proto", "../proto/vendor"],
    )?;

    // Tell Cargo to rerun this build script if any of the proto files change
    println!("cargo:rerun-if-changed=../proto");
    println!("cargo:rerun-if-changed=../proto/vendor");

    Ok(())
}
