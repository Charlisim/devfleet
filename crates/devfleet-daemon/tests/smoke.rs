use std::process::Command;

#[test]
fn daemon_smoke_entry_point_is_executable() {
    let output = Command::new(env!("CARGO_BIN_EXE_devfleetd"))
        .arg("--smoke")
        .output()
        .expect("daemon starts");
    assert!(output.status.success());
    devfleet_testkit::assert_smoke_output(&output.stdout, "daemon");
}
