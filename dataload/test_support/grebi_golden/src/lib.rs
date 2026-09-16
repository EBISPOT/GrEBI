//! Golden tests for the pipeline binaries.
//!
//! A case is a directory of recorded inputs and outputs, typically lifted from a
//! Nextflow work directory of one of the E2E test subgraphs. The test runs the
//! built binary in a scratch directory with the case's arguments, environment
//! and stdin, then compares its stdout and the files it wrote with the recorded
//! ones, byte for byte. `UPDATE_GOLDEN=1` rewrites the recorded outputs instead,
//! for when a change to the binary is intended; review the diff before
//! committing it.
//!
//! In arguments and environment values, `$CASE` stands for the case directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

static SCRATCH_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct GoldenCase {
    bin: PathBuf,
    case_dir: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, String)>,
    stdin: Option<String>,
    expected_stdout: Option<String>,
    outputs: Vec<String>,
    expect_failure: bool,
}

impl GoldenCase {
    /// `bin` is the binary's path, usually `env!("CARGO_BIN_EXE_<name>")`; `case_dir` is
    /// relative to the crate root, where cargo runs integration tests.
    pub fn new(bin: &str, case_dir: impl AsRef<Path>) -> Self {
        let case_dir = fs::canonicalize(case_dir.as_ref())
            .unwrap_or_else(|e| panic!("golden case directory {}: {}", case_dir.as_ref().display(), e));
        GoldenCase {
            bin: PathBuf::from(bin),
            case_dir,
            args: Vec::new(),
            envs: Vec::new(),
            stdin: None,
            expected_stdout: None,
            outputs: Vec::new(),
            expect_failure: false,
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I: IntoIterator<Item = S>, S: Into<String>>(mut self, args: I) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// A file in the case directory fed to the binary's stdin.
    pub fn stdin(mut self, file: &str) -> Self {
        self.stdin = Some(file.to_string());
        self
    }

    /// The file in the case directory the binary's stdout must match.
    pub fn stdout(mut self, file: &str) -> Self {
        self.expected_stdout = Some(file.to_string());
        self
    }

    /// A file the binary writes into its working directory, to match the file of
    /// the same name in the case directory.
    pub fn output(mut self, file: &str) -> Self {
        self.outputs.push(file.to_string());
        self
    }

    /// The binary is expected to exit with a failure (its output still compared).
    pub fn expect_failure(mut self) -> Self {
        self.expect_failure = true;
        self
    }

    fn expand(&self, s: &str) -> String {
        s.replace("$CASE", self.case_dir.to_str().unwrap())
    }

    pub fn run(self) {
        let update = std::env::var("UPDATE_GOLDEN").map(|v| v == "1").unwrap_or(false);
        let scratch = std::env::temp_dir().join(format!(
            "grebi_golden_{}_{}", std::process::id(), SCRATCH_COUNTER.fetch_add(1, Ordering::SeqCst)));
        fs::create_dir_all(&scratch).unwrap();

        let mut cmd = Command::new(&self.bin);
        cmd.current_dir(&scratch);
        for a in &self.args {
            cmd.arg(self.expand(a));
        }
        for (k, v) in &self.envs {
            cmd.env(k, self.expand(v));
        }
        cmd.stdin(match &self.stdin {
            Some(file) => Stdio::from(fs::File::open(self.case_dir.join(file))
                .unwrap_or_else(|e| panic!("stdin fixture {}: {}", file, e))),
            None => Stdio::null(),
        });
        let output = cmd.output().unwrap_or_else(|e| panic!("could not run {}: {}", self.bin.display(), e));
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() == self.expect_failure {
            panic!("{} exited with {} (expected {}):\n{}", self.bin.display(), output.status,
                if self.expect_failure { "failure" } else { "success" }, stderr);
        }

        let mut problems = Vec::new();
        if let Some(file) = &self.expected_stdout {
            check(&self.case_dir.join(file), &output.stdout, update, &mut problems);
        }
        for file in &self.outputs {
            let produced = fs::read(scratch.join(file))
                .unwrap_or_else(|e| panic!("{} did not write {}: {}\nstderr:\n{}", self.bin.display(), file, e, stderr));
            check(&self.case_dir.join(file), &produced, update, &mut problems);
        }
        let _ = fs::remove_dir_all(&scratch);
        if !problems.is_empty() {
            panic!("golden mismatch for {} (UPDATE_GOLDEN=1 rewrites the recorded files):\n{}\nstderr:\n{}",
                self.bin.display(), problems.join("\n"), stderr);
        }
    }
}

fn check(expected_path: &Path, actual: &[u8], update: bool, problems: &mut Vec<String>) {
    if update {
        fs::write(expected_path, actual).unwrap();
        return;
    }
    let expected = match fs::read(expected_path) {
        Ok(bytes) => bytes,
        Err(e) => {
            problems.push(format!("{}: cannot read the recorded file ({})", expected_path.display(), e));
            return;
        }
    };
    if expected != actual {
        problems.push(format!("{}:\n{}", expected_path.display(), describe_difference(&expected, actual)));
    }
}

/// The first differing line for text, the first differing byte otherwise.
fn describe_difference(expected: &[u8], actual: &[u8]) -> String {
    if let (Ok(e), Ok(a)) = (std::str::from_utf8(expected), std::str::from_utf8(actual)) {
        let (el, al): (Vec<&str>, Vec<&str>) = (e.lines().collect(), a.lines().collect());
        for i in 0..el.len().max(al.len()) {
            let (x, y) = (el.get(i), al.get(i));
            if x != y {
                return format!("  line {}:\n    recorded: {}\n    actual:   {}\n  ({} recorded lines, {} actual)",
                    i + 1, x.map(|s| truncate(s)).unwrap_or_else(|| "<none>".into()),
                    y.map(|s| truncate(s)).unwrap_or_else(|| "<none>".into()), el.len(), al.len());
            }
        }
        return "  same lines, different bytes (line endings?)".to_string();
    }
    let offset = expected.iter().zip(actual.iter()).position(|(x, y)| x != y).unwrap_or(expected.len().min(actual.len()));
    format!("  binary content differs from byte {} ({} recorded bytes, {} actual)", offset, expected.len(), actual.len())
}

fn truncate(s: &str) -> String {
    if s.len() > 300 { format!("{}...", &s[..300]) } else { s.to_string() }
}
