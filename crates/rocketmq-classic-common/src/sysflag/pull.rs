const FLAG_COMMIT_OFFSET: i32 = 0x1;
const FLAG_SUSPEND: i32 = 1 << 1;
const FLAG_SUBSCRIPTION: i32 = 1 << 2;
const FLAG_CLASS_FILTER: i32 = 1 << 3;
const FLAG_LITE_PULL_MESSAGE: i32 = 1 << 4;

pub fn build_pull_sys_flag(
    commit_offset: bool,
    suspend: bool,
    subscription: bool,
    class_filter: bool,
) -> i32 {
    build_pull_sys_flag_with_lite(commit_offset, suspend, subscription, class_filter, false)
}

pub fn build_pull_sys_flag_with_lite(
    commit_offset: bool,
    suspend: bool,
    subscription: bool,
    class_filter: bool,
    lite_pull: bool,
) -> i32 {
    (if commit_offset { FLAG_COMMIT_OFFSET } else { 0 })
        | (if suspend { FLAG_SUSPEND } else { 0 })
        | (if subscription { FLAG_SUBSCRIPTION } else { 0 })
        | (if class_filter { FLAG_CLASS_FILTER } else { 0 })
        | (if lite_pull { FLAG_LITE_PULL_MESSAGE } else { 0 })
}

pub fn has_commit_offset_flag(flag: i32) -> bool {
    flag & FLAG_COMMIT_OFFSET != 0
}
pub fn has_suspend_flag(flag: i32) -> bool {
    flag & FLAG_SUSPEND != 0
}
pub fn has_subscription_flag(flag: i32) -> bool {
    flag & FLAG_SUBSCRIPTION != 0
}
pub fn has_class_filter_flag(flag: i32) -> bool {
    flag & FLAG_CLASS_FILTER != 0
}
pub fn has_lite_pull_flag(flag: i32) -> bool {
    flag & FLAG_LITE_PULL_MESSAGE != 0
}
pub fn clear_commit_offset_flag(flag: i32) -> i32 {
    flag & !FLAG_COMMIT_OFFSET
}
pub fn clear_suspend_flag(flag: i32) -> i32 {
    flag & !FLAG_SUSPEND
}
pub fn build_sys_flag_with_subscription(flag: i32) -> i32 {
    flag | FLAG_SUBSCRIPTION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combines_and_clears_pull_flags_without_losing_unknown_bits() {
        let flag = build_pull_sys_flag_with_lite(true, true, false, true, true) | 0x4000;
        assert_eq!(flag, 0x401b);
        assert!(has_commit_offset_flag(flag));
        assert!(has_suspend_flag(flag));
        assert!(has_class_filter_flag(flag));
        assert!(has_lite_pull_flag(flag));
        assert_eq!(clear_commit_offset_flag(flag), flag & !FLAG_COMMIT_OFFSET);
        assert_eq!(clear_suspend_flag(flag) & 0x4000, 0x4000);
        assert_eq!(
            build_sys_flag_with_subscription(flag) & FLAG_SUBSCRIPTION,
            FLAG_SUBSCRIPTION
        );
    }
}
