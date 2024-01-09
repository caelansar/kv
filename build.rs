use std::process::Command;

fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.type_attribute(".", "#[derive(PartialOrd)]");
    config
        .out_dir("src/pb")
        .compile_protos(&["abi.proto"], &["."])
        .unwrap();

    Command::new("cargo").args(["fmt"]).output().unwrap();

    println!("cargo:rerun-if-changed=abi.proto");
}
