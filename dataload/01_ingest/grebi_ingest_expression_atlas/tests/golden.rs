use grebi_golden::GoldenCase;

const BIN: &str = env!("CARGO_BIN_EXE_grebi_ingest_expression_atlas");

fn case(dir: &str, threshold: &str, file: &str) -> GoldenCase {
    GoldenCase::new(BIN, format!("tests/golden/{}", dir))
        .args(["--min-median-tpm".to_string(), threshold.to_string(), "--".to_string(), format!("$CASE/{}", file)])
        .jsonl()
}

/// The Expression Atlas test subgraph's human experiment, matched as JSON
/// against what the Python script this replaced produced, including the
/// per-study counts it reports on stderr.
#[test]
fn matches_the_python_ingest_on_e_mtab_513() {
    case("test_expression_atlas", "0.5", "E-MTAB-513/E-MTAB-513-tpms.tsv")
        .stdout("E-MTAB-513.jsonl").stderr("E-MTAB-513.stderr.txt").run();
}

/// The test subgraph's synthetic experiment with excluded groups.
#[test]
fn matches_the_python_ingest_on_e_test_1() {
    case("test_expression_atlas", "0.5", "E-TEST-1/E-TEST-1-tpms.tsv")
        .stdout("E-TEST-1.jsonl").stderr("E-TEST-1.stderr.txt").run();
}

/// The plant experiment from the E2E data: plant anatomy and TAIR aliases.
#[test]
fn matches_the_python_ingest_on_the_plant_experiment() {
    case("plant", "0.5", "E-MTAB-4045/E-MTAB-4045-tpms.tsv").stdout("output.jsonl").stderr("stderr.txt").run();
}

/// A synthetic experiment exercising the rest of the rules: every exclusion
/// and control, mixed and unmapped anatomy and taxa, groups sharing an
/// anatomy, five-number summaries, missing values, columns in a different
/// order from the configuration, quoted free text, gene aliases, non-ASCII
/// labels and floats Python spells in exponent form.
#[test]
fn matches_the_python_ingest_on_every_branch() {
    case("branches", "0.5", "E-BRANCH-1/E-BRANCH-1-tpms.tsv").stdout("output.jsonl").stderr("stderr.txt").run();
}

/// The same experiment with a zero cutoff: zero medians stay excluded.
#[test]
fn matches_the_python_ingest_with_a_zero_cutoff() {
    case("branches", "0", "E-BRANCH-1/E-BRANCH-1-tpms.tsv")
        .stdout("output_threshold_0.jsonl").stderr("stderr_threshold_0.txt").run();
}

#[test]
fn refuses_a_differential_configuration() {
    case("not_baseline", "0.5", "E-DIFF-1/E-DIFF-1-tpms.tsv").expect_failure().run();
}

#[test]
fn refuses_a_broken_configuration() {
    case("broken_configuration", "0.5", "E-BAD-1/E-BAD-1-tpms.tsv").expect_failure().run();
}

#[test]
fn fails_without_the_companion_files() {
    case("missing_companions", "0.5", "E-STALE-1/E-STALE-1-tpms.tsv").expect_failure().run();
}

#[test]
fn refuses_anything_but_a_tpms_table() {
    case("wrong_suffix", "0.5", "E-MTAB-513-fpkms.tsv").expect_failure().run();
}

#[test]
fn refuses_negative_infinite_or_nan_cutoffs() {
    for threshold in ["-1", "inf", "nan"] {
        case("branches", threshold, "E-BRANCH-1/E-BRANCH-1-tpms.tsv").expect_failure().run();
    }
}

/// The cutoff is in the datasource config on purpose: there is no default.
#[test]
fn requires_the_cutoff() {
    GoldenCase::new(BIN, "tests/golden/branches")
        .args(["--", "$CASE/E-BRANCH-1/E-BRANCH-1-tpms.tsv"])
        .expect_failure()
        .run();
}
