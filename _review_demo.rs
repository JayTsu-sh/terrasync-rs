// 临时文件：仅用于验证 Claude Reviewer，合并验证后会删除。
fn parse_port(s: &str) -> u16 {
    // 故意违反项目规范：禁止 .unwrap()
    s.parse::<u16>().unwrap()
}
