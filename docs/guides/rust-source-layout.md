# Rust source layout

Keep production declarations before unit-test declarations in each source file. For this
workspace, unit tests use an inline `#[cfg(test)] mod tests` at the end of their owning file;
existing top-level test-only declarations also belong at the end. External unit-test modules
are valid Rust, but this repository keeps them inline for a consistent layout.

Keep Cargo integration tests in each crate's `tests/` directory. Shared test-support modules
remain separate when used across test targets. Preserve private module scope and compile-time
fixture paths when moving tests; do not expose production APIs just to make tests compile.

Use descriptive local names such as `header` for a scoped request-header builder. Avoid
migration-stage prefixes or generated counters such as `__phase18_header_1`. Historical plan,
fixture and execution-log identifiers are evidence and do not require source-name migration.
