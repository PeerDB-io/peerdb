use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");

    // path to workspace root
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_root = std::path::Path::new(&manifest_dir);
    let root = manifest_root.parent().unwrap().parent().unwrap();

    // protos are in <root>/protos/*.proto
    let protos = root.join("protos");

    let proto_files = std::fs::read_dir(protos)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path())
        .collect::<Vec<_>>();

    // compile all protos in <root>/protos by iterating over the directory
    prost_build::compile_protos(&proto_files, &[root.join("protos")])?;

    Ok(())
}
