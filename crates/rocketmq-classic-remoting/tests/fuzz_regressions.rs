#[path = "common/fuzz_harness.rs"]
mod fuzz_harness;

#[test]
fn bounded_fixture_mutations_preserve_decoder_safety() {
    let summary = fuzz_harness::run_fixed_seed_mutations();
    assert!(summary.frame_seeds > 0);
    assert!(summary.message_seeds > 0);
    assert!(summary.property_seeds > 0);
    assert!(summary.mutated_inputs > 0);
    assert!(summary.frame_decodes > 0);
    assert!(summary.message_decodes > 0);
    assert!(summary.property_decodes > 0);
}
