pub fn java_camel_to_snake(name: &str) -> String {
    let chars: Vec<_> = name.chars().collect();
    let mut snake = String::new();
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch.is_ascii_uppercase()
            && index > 0
            && (chars[index - 1].is_ascii_lowercase()
                || chars[index - 1].is_ascii_digit()
                || (chars[index - 1].is_ascii_uppercase()
                    && chars
                        .get(index + 1)
                        .is_some_and(|next| next.is_ascii_lowercase())))
        {
            snake.push('_');
        }
        snake.push(ch.to_ascii_lowercase());
    }
    snake
}

pub fn rust_header_method_exists(
    source: &str,
    trait_source: &str,
    rust_type: &str,
    method: &str,
) -> bool {
    if matches!(
        method,
        "get_and_check_not_null" | "write_if_not_null" | "write_int" | "write_long"
    ) {
        let trait_methods = trait_source
            .split_once("pub trait FastCodesHeader")
            .map(|(_, body)| body)
            .unwrap_or_default();
        return (trait_methods.contains(&format!("fn {method}("))
            || trait_methods.contains(&format!("fn {method}<")))
            && source.contains(&format!("impl super::FastCodesHeader for {rust_type} {{}}"));
    }

    if matches!(method, "encode_fields" | "decode_fields" | "check_fields") {
        let command_header = trait_source
            .split_once("pub trait FastCodesHeader")
            .map(|(command_header, _)| command_header)
            .unwrap_or(trait_source);
        return command_header.contains(&format!("fn {method}("));
    }

    let rpc_accessors = source
        .split_once("macro_rules! impl_rpc_request_header_accessors")
        .map(|(_, definitions)| definitions);
    if let Some(definitions) = rpc_accessors {
        let mut invocations = source;
        while let Some(start) = invocations.find("impl_rpc_request_header_accessors!(") {
            invocations = &invocations[start + "impl_rpc_request_header_accessors!(".len()..];
            let Some(end) = invocations.find(");") else {
                break;
            };
            let invocation = &invocations[..end];
            let names_type = invocation
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .any(|token| token == rust_type);
            if names_type && definitions.contains(&format!("pub fn {method}(")) {
                return true;
            }
            invocations = &invocations[end + 2..];
        }
    }

    let topic_accessors = source
        .split_once("macro_rules! impl_topic_request_header_accessors")
        .map(|(_, definitions)| definitions);
    if let Some(definitions) = topic_accessors {
        let rpc_definitions = source
            .split_once("macro_rules! impl_rpc_request_header_accessors")
            .map(|(_, definitions)| definitions)
            .unwrap_or_default();
        let mut invocations = source;
        while let Some(start) = invocations.find("impl_topic_request_header_accessors!(") {
            invocations = &invocations[start + "impl_topic_request_header_accessors!(".len()..];
            let Some(end) = invocations.find(");") else {
                break;
            };
            let invocation = &invocations[..end];
            let names_type = invocation
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .any(|token| token == rust_type);
            if names_type
                && (definitions.contains(&format!("pub fn {method}("))
                    || rpc_definitions.contains(&format!("pub fn {method}(")))
            {
                return true;
            }
            invocations = &invocations[end + 2..];
        }
    }

    let mut remaining = source;
    while let Some(start) = remaining.find("impl_header_field_accessors!(") {
        remaining = &remaining[start + "impl_header_field_accessors!(".len()..];
        let Some(end) = remaining.find(");") else {
            break;
        };
        let block = &remaining[..end];
        if block.trim_start().starts_with(&format!("{rust_type},"))
            && block
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .any(|token| token == method)
        {
            return true;
        }
        remaining = &remaining[end + 2..];
    }

    let implementation = format!("impl {rust_type} {{");
    source
        .split_once(&implementation)
        .and_then(|(_, block)| block.split_once("\n}\n"))
        .is_some_and(|(block, _)| block.contains(&format!("pub fn {method}(")))
}
