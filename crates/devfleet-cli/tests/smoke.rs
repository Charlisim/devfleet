use std::process::Command;

#[test]
fn cli_smoke_entry_point_is_executable() {
    let output = Command::new(env!("CARGO_BIN_EXE_devfleet"))
        .arg("--smoke")
        .output()
        .expect("cli starts");
    assert!(output.status.success());
    devfleet_testkit::assert_smoke_output(&output.stdout, "cli");
}
