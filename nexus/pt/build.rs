use std::{env, io::Result, path::PathBuf};

fn main() -> Result<()> {
    // path to workspace root
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_root = std::path::Path::new(&manifest_dir);
    let root = manifest_root.parent().unwrap().parent().unwrap();
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // protos are in <root>/protos/*.proto
    let protos = root.join("protos");

    let proto_files = std::fs::read_dir(protos)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path())
        .collect::<Vec<_>>();

    // iterate and print all the proto files
    for proto in &proto_files {
        println!("cargo:warning={}", proto.display());
    }

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // for older systems
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("store_descriptor.bin"))
        .out_dir("./src")
        .compile(&proto_files, &[root.join("protos")])?;

    Ok(())
}
