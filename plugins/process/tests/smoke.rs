use std::process::Command;

#[test]
fn process_plugin_smoke_entry_point_is_executable() {
    let output = Command::new(env!("CARGO_BIN_EXE_devfleet-plugin-process"))
        .arg("--smoke")
        .output()
        .expect("plugin starts");
    assert!(output.status.success());
    let descriptor: devfleet_plugin_sdk::ProviderDescriptor =
        serde_json::from_slice(&output.stdout).expect("descriptor JSON");
    assert_eq!(descriptor.id, "official.process");
}
