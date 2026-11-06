#[path = "../tests/common/fuzz_harness.rs"]
mod fuzz_harness;

fn main() {
    let summary = fuzz_harness::run_fixed_seed_mutations();
    println!(
        "fuzz smoke passed: {} frame seeds, {} message seeds, {} property seeds, {} bounded mutations, {} frame decodes, {} message decodes, {} property decodes",
        summary.frame_seeds,
        summary.message_seeds,
        summary.property_seeds,
        summary.mutated_inputs,
        summary.frame_decodes,
        summary.message_decodes,
        summary.property_decodes,
    );
}
