fn main() {
    println!("cargo:rustc-check-cfg=cfg(rocketmq_classic_test_support)");
}
