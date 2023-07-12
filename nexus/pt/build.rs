use std::io::Result;

fn main() -> Result<()> {
    // path to workspace root
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_root = std::path::Path::new(&manifest_dir);
    let root = manifest_root.parent().unwrap().parent().unwrap();

    // protos are in <root>/protos/*.proto
    let protos = root.join("protos");

    let proto_files = std::fs::read_dir(protos)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        // ignore flow.proto file
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s != "flow.proto")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect::<Vec<_>>();

    // iterate and print all the proto files
    for proto in &proto_files {
        println!("cargo:warning={}", proto.display());
    }

    // see: https://github.com/tokio-rs/prost/issues/75 for future.
    let mut config = prost_build::Config::new();
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.type_attribute(".", "#[serde(rename_all = \"camelCase\")]");

    // generate rust code for all protos in <root>/protos
    config.compile_protos(&proto_files, &[root.join("protos")])?;

    Ok(())
}
