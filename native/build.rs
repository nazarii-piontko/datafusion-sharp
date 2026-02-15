use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    build_flat_buffers();
}

fn build_flat_buffers() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    // Only run on Linux x86_64
    if target_os != "linux" || target_arch != "x86_64" {
        println!("cargo:warning=Skipping flatc generation (not linux x86_64)");
        return;
    }

    println!("cargo:rerun-if-changed=../wire");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    let flatc_path = manifest_dir.join("../bin/flatc-x64");
    let schema_dir = manifest_dir.join("../wire");
    let out_dir = manifest_dir.join("src/wire");

    if !flatc_path.exists() {
        panic!("flatc compiler not found at {}", flatc_path.display());
    }

    if !schema_dir.exists() {
        panic!("FlatBuffers schema directory not found at {}", schema_dir.display());
    }

    if !out_dir.exists() {
        panic!("FlatBuffers compilation output directory not found at {}", out_dir.display());
    }

    let entries = fs::read_dir(&schema_dir).expect("Failed to read FlatBuffers schema directory");
    let mut processed_files = 0;

    let mut flatc_command = Command::new(&flatc_path);
    flatc_command
        .arg("--rust")
        .arg("-o").arg(&out_dir)
        .arg("--rust-module-root-file");

    for entry in entries {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) == Some("fbs") {
            println!("cargo:rerun-if-changed={}", path.display());

            flatc_command.arg(&path);

            processed_files += 1;
        }
    }

    if processed_files == 0 {
        println!("cargo:warning=No .fbs files found in {}", schema_dir.display());
    } else {
        println!("flatc command: {:?}", flatc_command);

        let status = flatc_command
            .status()
            .expect("Failed to execute flatc");

        if !status.success() {
            panic!("flatc failed with exit code: {}", status);
        }

        println!("cargo:info=Processed {} .fbs files", processed_files);
    }
}
