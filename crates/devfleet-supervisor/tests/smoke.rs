use std::process::Command;

#[test]
fn supervisor_smoke_entry_point_is_executable() {
    let output = Command::new(env!("CARGO_BIN_EXE_devfleet-supervisor"))
        .arg("--smoke")
        .output()
        .expect("supervisor starts");
    assert!(output.status.success());
    devfleet_testkit::assert_smoke_output(&output.stdout, "supervisor");
}
