use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tiny_http::{Response, Server};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const APP: &str = "validation/ios-isolation/app";
const BUNDLE: &str = "org.reactjs.native.example.IsolationApp";
const RUNTIME: &str = "com.apple.CoreSimulator.SimRuntime.iOS-26-4";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Report {
    native_environment: String,
    bundle_environment: String,
    backend_environment: String,
    build_id: String,
    revision: String,
    nonce: String,
}
#[derive(Clone)]
struct Expected {
    environment: String,
    build_id: String,
    nonce: String,
}
fn verdict(r: &Report, e: &Expected) -> &'static str {
    if r.nonce != e.nonce {
        return "STALE_REPORT";
    }
    if r.native_environment != e.environment
        || r.bundle_environment != e.environment
        || r.backend_environment != e.environment
        || r.build_id != e.build_id
    {
        return "IDENTITY_MISMATCH";
    }
    "IDENTITY_VERIFIED"
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn command(program: &str, args: &[&str], cwd: &Path) -> Result<String> {
    let output = Command::new(program).args(args).current_dir(cwd).output()?;
    if !output.status.success() {
        return Err(format!(
            "{program} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
fn logged(program: &str, args: &[&str], cwd: &Path, log: &Path) -> Result<()> {
    eprintln!("{program} {args:?}; log={}", log.display());
    let file = fs::File::create(log)?;
    let status = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stdout(file.try_clone()?)
        .stderr(file)
        .status()?;
    if !status.success() {
        return Err(format!("{program} failed: see {}", log.display()).into());
    }
    Ok(())
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Environment {
    id: String,
    worktree: PathBuf,
    udid: String,
    metro: u16,
    backend: u16,
    report: u16,
}
impl Environment {
    fn app(&self) -> PathBuf {
        self.worktree.join(APP)
    }
}
fn native_source(e: &Environment, metro: u16, backend: u16) -> Result<(String, String)> {
    let template = fs::read_to_string(e.app().join("ios/IsolationApp/AppDelegate.swift.in"))?;
    let source = template
        .replace("@@ENVIRONMENT@@", &e.id)
        .replace("@@METRO@@", &metro.to_string())
        .replace("@@BACKEND@@", &backend.to_string())
        .replace("@@REPORT@@", &e.report.to_string());
    let build = hash(source.as_bytes());
    Ok((source.replace("@@BUILD@@", &build), build))
}
fn generate(e: &Environment, metro: u16, backend: u16) -> Result<String> {
    let (source, build) = native_source(e, metro, backend)?;
    fs::write(e.app().join("ios/IsolationApp/AppDelegate.swift"), source)?;
    Ok(build)
}
fn write_revision(e: &Environment, value: &str) -> Result<()> {
    fs::write(
        e.app().join("revision.js"),
        format!("export default {};\n", json!({"value":value})),
    )?;
    Ok(())
}
fn prepare(root: &Path, state: &Path) -> Result<()> {
    command("pod", &["--version"], root)?;
    command("npm", &["--version"], root)?;
    if state.exists() {
        return Err("State directory already exists; choose a fresh directory".into());
    }
    fs::create_dir_all(state)?;
    let state = state.canonicalize()?;
    let mut environments = Vec::new();
    for (i, id) in ["A", "B"].iter().enumerate() {
        let worktree = state.join(id);
        command(
            "git",
            &[
                "worktree",
                "add",
                "--detach",
                worktree.to_str().ok_or("Invalid path")?,
                "HEAD",
            ],
            root,
        )?;
        let udid = command(
            "xcrun",
            &[
                "simctl",
                "create",
                &format!("DevFleet-fixture-{id}-{}", std::process::id()),
                "com.apple.CoreSimulator.SimDeviceType.iPhone-17",
                RUNTIME,
            ],
            root,
        )?;
        let e = Environment {
            id: id.to_string(),
            worktree,
            udid,
            metro: 18081 + i as u16,
            backend: 19081 + i as u16,
            report: 19181 + i as u16,
        };
        // Persist owned resources before dependency installation can fail.
        environments.push(e);
        write_json(&state.join("environments.json"), &environments)?;
        let e = environments.last().unwrap();
        generate(e, e.metro, e.backend)?;
        write_json(&e.app().join("fixture.json"), &json!({"environment":e.id}))?;
        write_revision(e, "initial")?;
        logged(
            "npm",
            &["ci"],
            &e.app(),
            &state.join(format!("{}-npm.log", e.id)),
        )?;
        logged(
            "pod",
            &["install", "--deployment"],
            &e.app().join("ios"),
            &state.join(format!("{}-pods.log", e.id)),
        )?;
        logged(
            "npm",
            &["run", "typecheck"],
            &e.app(),
            &state.join(format!("{}-typecheck.log", e.id)),
        )?;
    }
    Ok(())
}
struct Processes(Vec<Child>);
impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
type Reports = Arc<Mutex<Vec<Report>>>;
fn servers(e: &Environment, expected: Arc<Mutex<Expected>>, reports: Reports) -> Result<()> {
    let backend = Server::http(("127.0.0.1", e.backend)).map_err(|e| e.to_string())?;
    let report = Server::http(("127.0.0.1", e.report)).map_err(|e| e.to_string())?;
    let id = e.id.clone();
    thread::spawn(move || {
        for req in backend.incoming_requests() {
            let response = if req.method().as_str() == "GET" && req.url() == "/identity" {
                Response::from_string(json!({"environment":id}).to_string())
            } else {
                Response::from_string("Not found").with_status_code(404)
            };
            let _ = req.respond(response);
        }
    });
    thread::spawn(move || {
        for mut req in report.incoming_requests() {
            if req.method().as_str() != "POST" || req.url() != "/report" {
                let _ = req.respond(Response::empty(404));
                continue;
            }
            let mut bytes = Vec::new();
            let _ = req.as_reader().take(8193).read_to_end(&mut bytes);
            if bytes.len() > 8192 {
                let _ = req.respond(Response::empty(413));
                continue;
            }
            match serde_json::from_slice::<Report>(&bytes) {
                Ok(r) => {
                    let status = verdict(&r, &expected.lock().unwrap());
                    if status != "STALE_REPORT" {
                        let mut saved = reports.lock().unwrap();
                        if saved.len() == 128 {
                            saved.remove(0);
                        }
                        saved.push(r);
                    }
                    let _ = req.respond(Response::from_string(status));
                }
                Err(_) => {
                    let _ = req.respond(Response::empty(400));
                }
            }
        }
    });
    Ok(())
}
fn build(e: &Environment, state: &Path, phase: &str) -> Result<()> {
    let derived = state.join(format!("{}-build", e.id));
    let destination = format!("platform=iOS Simulator,id={}", e.udid);
    logged(
        "xcodebuild",
        &[
            "-workspace",
            "IsolationApp.xcworkspace",
            "-scheme",
            "IsolationApp",
            "-configuration",
            "Debug",
            "-destination",
            &destination,
            "-derivedDataPath",
            derived.to_str().ok_or("Invalid build path")?,
            "-jobs",
            "2",
            "CODE_SIGNING_ALLOWED=NO",
            "build",
        ],
        &e.app().join("ios"),
        &state.join(format!("{}-{phase}-build.log", e.id)),
    )?;
    command(
        "xcrun",
        &[
            "simctl",
            "install",
            &e.udid,
            derived
                .join("Build/Products/Debug-iphonesimulator/IsolationApp.app")
                .to_str()
                .ok_or("Invalid app path")?,
        ],
        &e.app(),
    )?;
    Ok(())
}
fn launch(e: &Environment, expected: &Arc<Mutex<Expected>>, reports: &Reports) -> Result<String> {
    let nonce = command("uuidgen", &[], &e.app())?;
    expected.lock().unwrap().nonce = nonce.clone();
    reports.lock().unwrap().clear();
    let output = Command::new("xcrun")
        .args([
            "simctl",
            "launch",
            "--terminate-running-process",
            &e.udid,
            BUNDLE,
        ])
        .env("SIMCTL_CHILD_FLEET_NONCE", &nonce)
        .output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string().into());
    }
    Ok(nonce)
}
fn wait_report(reports: &Reports, nonce: &str, revision: &str) -> Result<Report> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(120) {
        if let Some(r) = reports
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|r| r.nonce == nonce && r.revision == revision)
        {
            return Ok(r.clone());
        }
        thread::sleep(Duration::from_millis(250));
    }
    Err(format!("No app report for revision {revision} within 120s").into())
}
fn binary_hash(e: &Environment, state: &Path) -> Result<String> {
    Ok(hash(&fs::read(state.join(format!(
        "{}-build/Build/Products/Debug-iphonesimulator/IsolationApp.app/IsolationApp.debug.dylib",
        e.id
    )))?))
}
fn record(
    e: &Environment,
    state: &Path,
    phase: &str,
    r: &Report,
    expected: &Arc<Mutex<Expected>>,
    wanted: &str,
    evidence: &mut Vec<Value>,
) -> Result<()> {
    let status = verdict(r, &expected.lock().unwrap());
    let entry = json!({"phase":phase,"environment":e.id,"udid":e.udid,"worktree":e.worktree,
        "head":command("git", &["rev-parse","HEAD"], &e.worktree)?,"report":r,"verdict":status,
        "binary_sha256":binary_hash(e,state)?,
        "native_source_sha256":hash(&fs::read(e.app().join("ios/IsolationApp/AppDelegate.swift"))?),
        "revision_sha256":hash(&fs::read(e.app().join("revision.js"))?),
        "npm_lock_sha256":hash(&fs::read(e.app().join("package-lock.json"))?),
        "pod_lock_sha256":hash(&fs::read(e.app().join("ios/Podfile.lock"))?),"timestamp_unix":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()});
    evidence.push(entry);
    write_json(&state.join("evidence.json"), evidence)?;
    if status != wanted {
        return Err(format!("{phase}: expected {wanted}, got {status}").into());
    }
    command(
        "xcrun",
        &[
            "simctl",
            "io",
            &e.udid,
            "screenshot",
            state
                .join(format!("{}-{phase}.png", e.id))
                .to_str()
                .ok_or("Invalid screenshot path")?,
        ],
        &e.app(),
    )?;
    eprintln!("{} {phase}: {status}", e.id);
    Ok(())
}
fn experiment(state: &Path) -> Result<()> {
    let environments: Vec<Environment> =
        serde_json::from_slice(&fs::read(state.join("environments.json"))?)?;
    if environments.len() != 2 {
        return Err("Need exactly two prepared environments".into());
    }
    if command("sw_vers", &["-productVersion"], state)? != "26.6.2"
        || command("uname", &["-m"], state)? != "arm64"
        || command("xcodebuild", &["-version"], state)? != "Xcode 26.4.1\nBuild version 17E202"
    {
        return Err("Host differs from pinned macOS arm64 / Xcode validation matrix".into());
    }
    let mut processes = Processes(Vec::new());
    let mut contexts = Vec::new();
    let started = Instant::now();
    let mut evidence = Vec::new();
    for e in &environments {
        let build_id = generate(e, e.metro, e.backend)?;
        write_revision(e, "initial")?;
        let expected = Arc::new(Mutex::new(Expected {
            environment: e.id.clone(),
            build_id,
            nonce: String::new(),
        }));
        let reports: Reports = Arc::new(Mutex::new(Vec::new()));
        servers(e, expected.clone(), reports.clone())?;
        if TcpStream::connect(("127.0.0.1", e.metro)).is_ok() {
            return Err(format!("Metro port {} occupied", e.metro).into());
        }
        let log = fs::File::create(state.join(format!("{}-metro.log", e.id)))?;
        processes.0.push(
            Command::new("node")
                .args([
                    "node_modules/react-native/cli.js",
                    "start",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    &e.metro.to_string(),
                    "--no-interactive",
                ])
                .current_dir(e.app())
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
        let start = Instant::now();
        while TcpStream::connect(("127.0.0.1", e.metro)).is_err() {
            if start.elapsed() > Duration::from_secs(60) {
                return Err("Metro startup timed out".into());
            }
            thread::sleep(Duration::from_millis(250));
        }
        let devices: Value = serde_json::from_str(&command(
            "xcrun",
            &["simctl", "list", "devices", "-j"],
            &e.app(),
        )?)?;
        let device = devices["devices"][RUNTIME]
            .as_array()
            .and_then(|ds| ds.iter().find(|d| d["udid"] == e.udid))
            .ok_or("Pinned simulator missing")?;
        if device["state"] != "Booted" {
            command("xcrun", &["simctl", "boot", &e.udid], &e.app())?;
        }
        command("xcrun", &["simctl", "bootstatus", &e.udid, "-b"], &e.app())?;
        build(e, state, "initial")?;
        let nonce = launch(e, &expected, &reports)?;
        let r = wait_report(&reports, &nonce, "initial")?;
        record(
            e,
            state,
            "initial",
            &r,
            &expected,
            "IDENTITY_VERIFIED",
            &mut evidence,
        )?;
        contexts.push((expected, reports));
    }
    let a = &environments[0];
    let b = &environments[1];
    let (ae, ar) = &contexts[0];
    let (be, br) = &contexts[1];
    let original_binary = binary_hash(a, state)?;
    let original_b_binary = binary_hash(b, state)?;
    write_revision(a, "edited-A")?;
    let nonce = launch(a, ae, ar)?;
    let r = wait_report(ar, &nonce, "edited-A")?;
    record(
        a,
        state,
        "js-edit",
        &r,
        ae,
        "IDENTITY_VERIFIED",
        &mut evidence,
    )?;
    let nonce = launch(b, be, br)?;
    let r = wait_report(br, &nonce, "initial")?;
    record(
        b,
        state,
        "js-unchanged",
        &r,
        be,
        "IDENTITY_VERIFIED",
        &mut evidence,
    )?;
    if original_binary != binary_hash(a, state)? {
        return Err("JS edit changed native binary".into());
    }
    // Changing generated compiled source cannot change the installed app.
    let cross_backend_build = generate(a, a.metro, b.backend)?;
    let nonce = launch(a, ae, ar)?;
    let r = wait_report(ar, &nonce, "edited-A")?;
    record(
        a,
        state,
        "endpoint-without-build",
        &r,
        ae,
        "IDENTITY_VERIFIED",
        &mut evidence,
    )?;
    if original_binary != binary_hash(a, state)? {
        return Err("Unexpected binary change".into());
    }
    build(a, state, "cross-backend")?;
    ae.lock().unwrap().build_id = cross_backend_build;
    let nonce = launch(a, ae, ar)?;
    let r = wait_report(ar, &nonce, "edited-A")?;
    if r.backend_environment != b.id || r.bundle_environment != a.id {
        return Err("Cross-backend failed for the wrong reason".into());
    }
    record(
        a,
        state,
        "cross-backend",
        &r,
        ae,
        "IDENTITY_MISMATCH",
        &mut evidence,
    )?;
    if original_binary == binary_hash(a, state)? {
        return Err("Compiled endpoint did not change binary".into());
    }
    let cross_metro_build = generate(a, b.metro, a.backend)?;
    build(a, state, "cross-metro")?;
    ae.lock().unwrap().build_id = cross_metro_build;
    let nonce = launch(a, ae, ar)?;
    let r = wait_report(ar, &nonce, "initial")?;
    if r.bundle_environment != b.id || r.backend_environment != a.id {
        return Err("Cross-Metro failed for the wrong reason".into());
    }
    record(
        a,
        state,
        "cross-metro",
        &r,
        ae,
        "IDENTITY_MISMATCH",
        &mut evidence,
    )?;
    let restored = generate(a, a.metro, a.backend)?;
    build(a, state, "restored")?;
    ae.lock().unwrap().build_id = restored;
    let nonce = launch(a, ae, ar)?;
    let r = wait_report(ar, &nonce, "edited-A")?;
    record(
        a,
        state,
        "restored",
        &r,
        ae,
        "IDENTITY_VERIFIED",
        &mut evidence,
    )?;
    let nonce = launch(b, be, br)?;
    let r = wait_report(br, &nonce, "initial")?;
    record(
        b,
        state,
        "final-unchanged",
        &r,
        be,
        "IDENTITY_VERIFIED",
        &mut evidence,
    )?;
    if original_b_binary != binary_hash(b, state)? {
        return Err("B native binary changed".into());
    }
    write_json(
        &state.join("result.json"),
        &json!({"passed":true,"status":"passed","elapsed_seconds":started.elapsed().as_secs(),
        "macos":command("sw_vers",&[],state)?,"architecture":command("uname",&["-m"],state)?,
        "xcode":command("xcodebuild",&["-version"],state)?,"runtime":RUNTIME,"react_native":"0.87.1",
        "node":command("node",&["--version"],state)?,"rust":command("rustc",&["--version"],state)?}),
    )?;
    Ok(())
}
fn run(state: &Path) -> Result<()> {
    write_json(
        &state.join("result.json"),
        &json!({"passed":false,"status":"running"}),
    )?;
    write_json(&state.join("evidence.json"), &Vec::<Value>::new())?;
    let result = experiment(state);
    if let Err(error) = &result {
        write_json(
            &state.join("result.json"),
            &json!({"passed":false,"status":"failed","error":error.to_string()}),
        )?;
    }
    result
}
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("prepare") if args.len() == 4 => prepare(
            &PathBuf::from(&args[2]).canonicalize()?,
            &PathBuf::from(&args[3]),
        ),
        Some("run") if args.len() == 3 => run(&PathBuf::from(&args[2]).canonicalize()?),
        _ => Err("Usage: ios-isolation prepare REPO NEW_STATE_DIR | run STATE_DIR".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Report, Expected) {
        (
            Report {
                native_environment: "A".into(),
                bundle_environment: "A".into(),
                backend_environment: "A".into(),
                build_id: "build".into(),
                revision: "initial".into(),
                nonce: "fresh".into(),
            },
            Expected {
                environment: "A".into(),
                build_id: "build".into(),
                nonce: "fresh".into(),
            },
        )
    }
    #[test]
    fn failed_run_invalidates_previous_success() {
        let state =
            std::env::temp_dir().join(format!("devfleet-regression-{}", std::process::id()));
        fs::create_dir_all(&state).unwrap();
        write_json(&state.join("result.json"), &json!({"passed":true})).unwrap();
        assert!(run(&state).is_err());
        let result: Value =
            serde_json::from_slice(&fs::read(state.join("result.json")).unwrap()).unwrap();
        assert_eq!(result["passed"], false);
        assert_eq!(result["status"], "failed");
    }
    #[test]
    fn verifies_matching_report() {
        let (r, e) = fixture();
        assert_eq!(verdict(&r, &e), "IDENTITY_VERIFIED");
    }
    #[test]
    fn detects_crossed_bundle() {
        let (mut r, e) = fixture();
        r.bundle_environment = "B".into();
        assert_eq!(verdict(&r, &e), "IDENTITY_MISMATCH");
    }
    #[test]
    fn detects_crossed_backend() {
        let (mut r, e) = fixture();
        r.backend_environment = "B".into();
        assert_eq!(verdict(&r, &e), "IDENTITY_MISMATCH");
    }
    #[test]
    fn rejects_previous_launch() {
        let (mut r, e) = fixture();
        r.nonce = "old".into();
        assert_eq!(verdict(&r, &e), "STALE_REPORT");
    }
    #[test]
    fn rejects_previous_build() {
        let (mut r, e) = fixture();
        r.build_id = "old".into();
        assert_eq!(verdict(&r, &e), "IDENTITY_MISMATCH");
    }
    #[test]
    fn rejects_wrong_native_environment() {
        let (mut r, e) = fixture();
        r.native_environment = "B".into();
        assert_eq!(verdict(&r, &e), "IDENTITY_MISMATCH");
    }

    #[test]
    fn checked_in_hardware_evidence_covers_isolation_and_crosswires() {
        let evidence_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("evidence");
        let result: Value = serde_json::from_slice(
            &fs::read(evidence_root.join("result.json")).expect("checked-in result reads"),
        )
        .expect("checked-in result parses");
        assert_eq!(result["passed"], true);
        assert_eq!(result["status"], "passed");

        let environments: Vec<Value> = serde_json::from_slice(
            &fs::read(evidence_root.join("environments.json"))
                .expect("checked-in environments read"),
        )
        .expect("checked-in environments parse");
        assert_eq!(environments.len(), 2);
        assert_eq!(environments[0]["id"], "A");
        assert_eq!(environments[1]["id"], "B");
        assert_ne!(environments[0]["udid"], environments[1]["udid"]);
        assert_ne!(environments[0]["metro"], environments[1]["metro"]);
        assert_ne!(environments[0]["backend"], environments[1]["backend"]);

        let evidence: Vec<Value> = serde_json::from_slice(
            &fs::read(evidence_root.join("evidence.json")).expect("checked-in evidence reads"),
        )
        .expect("checked-in evidence parses");
        for (environment, phase, verdict) in [
            ("A", "initial", "IDENTITY_VERIFIED"),
            ("B", "initial", "IDENTITY_VERIFIED"),
            ("A", "js-edit", "IDENTITY_VERIFIED"),
            ("B", "js-unchanged", "IDENTITY_VERIFIED"),
            ("A", "endpoint-without-build", "IDENTITY_VERIFIED"),
            ("A", "cross-backend", "IDENTITY_MISMATCH"),
            ("A", "cross-metro", "IDENTITY_MISMATCH"),
            ("A", "restored", "IDENTITY_VERIFIED"),
            ("B", "final-unchanged", "IDENTITY_VERIFIED"),
        ] {
            assert!(evidence.iter().any(|entry| {
                entry["environment"] == environment
                    && entry["phase"] == phase
                    && entry["verdict"] == verdict
            }));
        }
    }
}
