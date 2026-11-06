#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::{
    ffi::{CStr, CString},
    fs, io,
    mem::size_of,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        raw::{c_char, c_int, c_short, c_uchar, c_ulong, c_ushort},
    },
    path::Path,
    ptr,
};

use super::{Address, eligible, java_linux_ipv6_scope, java_unix_interface_root_name};

const AF_INET: c_int = 2;
const SOCK_DGRAM: c_int = 2;
const SIOCGIFCONF: c_ulong = 0x8912;
const SIOCGIFFLAGS: c_ulong = 0x8913;
const IFNAMSIZ: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct IfMap {
    mem_start: c_ulong,
    mem_end: c_ulong,
    base_addr: c_ushort,
    irq: c_uchar,
    dma: c_uchar,
    port: c_uchar,
}

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct SockAddrIn {
    family: c_ushort,
    port: c_ushort,
    address: u32,
    zero: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(dead_code)]
union IfReqData {
    address_in: SockAddrIn,
    flags: c_short,
    index: c_int,
    mtu: c_int,
    map: IfMap,
    slave: [c_char; IFNAMSIZ],
    new_name: [c_char; IFNAMSIZ],
    data: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IfReq {
    name: [c_char; IFNAMSIZ],
    data: IfReqData,
}

#[repr(C)]
struct IfConf {
    length: c_int,
    buffer: *mut c_char,
}

unsafe extern "C" {
    fn socket(domain: c_int, socket_type: c_int, protocol: c_int) -> c_int;
    fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
}

#[derive(Debug)]
struct Candidate {
    interface: String,
    address: Address,
}

#[derive(Default)]
struct InterfaceAddresses {
    name: String,
    ipv4: Vec<Address>,
    ipv6: Vec<Address>,
}

fn open_ipv4_socket() -> io::Result<OwnedFd> {
    // SAFETY: socket has no pointer arguments and returns a new owned descriptor.
    let fd = unsafe { socket(AF_INET, SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this function takes ownership of the new descriptor exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn ifreq(name: &str) -> io::Result<IfReq> {
    let name =
        CString::new(name).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    if name.as_bytes().len() >= IFNAMSIZ {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name exceeds Linux IFNAMSIZ",
        ));
    }
    let mut row = IfReq {
        name: [0; IFNAMSIZ],
        data: IfReqData { flags: 0 },
    };
    for (target, source) in row.name.iter_mut().zip(name.as_bytes()) {
        *target = *source as c_char;
    }
    Ok(row)
}

fn get_flags(fd: c_int, name: &str) -> io::Result<u32> {
    let mut row = ifreq(name)?;
    // SAFETY: row matches Linux ifreq layout and is writable for SIOCGIFFLAGS.
    if unsafe { ioctl(fd, SIOCGIFFLAGS, &mut row) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // Linux/JDK mask the short to unsigned 16-bit interface flags.
    Ok(unsafe { row.data.flags } as u16 as u32)
}

fn alias_root(fd: c_int, name: &str) -> Option<String> {
    let parent = name.split_once(':')?.0;
    if parent.is_empty() || get_flags(fd, parent).is_err() {
        return None;
    }
    java_unix_interface_root_name(name, true).map(str::to_owned)
}

fn interface_root(fd: c_int, name: &str) -> Option<String> {
    if name.contains(':') {
        return alias_root(fd, name);
    }
    java_unix_interface_root_name(name, true).map(str::to_owned)
}

fn ipv4_table(fd: c_int) -> io::Result<Vec<Candidate>> {
    let mut config = IfConf {
        length: 0,
        buffer: ptr::null_mut(),
    };
    // SAFETY: a null buffer performs Linux's documented SIOCGIFCONF size probe.
    if unsafe { ioctl(fd, SIOCGIFCONF, &mut config) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let required = usize::try_from(config.length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "negative SIOCGIFCONF buffer size",
        )
    })?;
    let mut buffer = vec![0u8; required];
    config.buffer = buffer.as_mut_ptr().cast();
    // SAFETY: `buffer` owns the writable byte range described by `config`.
    if unsafe { ioctl(fd, SIOCGIFCONF, &mut config) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let written = usize::try_from(config.length)
        .unwrap_or(0)
        .min(buffer.len());
    let mut candidates = Vec::new();
    for bytes in buffer[..written].chunks_exact(size_of::<IfReq>()) {
        // SAFETY: each complete record is initialized by SIOCGIFCONF; unaligned
        // reads avoid assuming Vec<u8> alignment.
        let row = unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<IfReq>()) };
        // SAFETY: Linux ifreq names are NUL-terminated within IFNAMSIZ bytes.
        let native_name = unsafe { CStr::from_ptr(row.name.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: this member overlays Linux ifr_addr, populated in every row.
        let address = unsafe { row.data.address_in };
        if native_name.is_empty() || address.family as c_int != AF_INET {
            continue;
        }
        let Some(interface) = interface_root(fd, &native_name) else {
            continue;
        };
        candidates.push(Candidate {
            interface,
            address: Address {
                ip: IpAddr::V4(Ipv4Addr::from(address.address.to_ne_bytes())),
                scope: 0,
                scope_name: None,
            },
        });
    }
    Ok(candidates)
}

fn parse_ipv6_row(line: &str) -> Option<Candidate> {
    let mut fields = line.split_whitespace();
    let address_hex = fields.next()?;
    let interface_index = u32::from_str_radix(fields.next()?, 16).ok()?;
    let _prefix = fields.next()?;
    let _scope = fields.next()?;
    let _dad_status = fields.next()?;
    let native_name = fields.next()?;
    if address_hex.len() != 32 {
        return None;
    }
    let interface = native_name.to_owned();
    let mut octets = [0u8; 16];
    for (index, word) in address_hex.as_bytes().chunks_exact(4).enumerate() {
        let word = std::str::from_utf8(word).ok()?;
        let value = u16::from_str_radix(word, 16).ok()?;
        octets[index * 2..index * 2 + 2].copy_from_slice(&value.to_be_bytes());
    }
    let scope = java_linux_ipv6_scope(0, interface_index);
    Some(Candidate {
        address: Address {
            ip: IpAddr::V6(Ipv6Addr::from(octets)),
            scope,
            scope_name: (scope != 0).then(|| interface.clone()),
        },
        interface,
    })
}

fn ipv6_table(fd: c_int) -> Vec<Candidate> {
    // OpenJDK silently leaves its current interface list unchanged when this
    // Linux proc file cannot be opened.
    let contents = fs::read_to_string("/proc/net/if_inet6").unwrap_or_default();
    contents
        .lines()
        .filter_map(parse_ipv6_row)
        .filter_map(|mut candidate| {
            if candidate.interface.contains(':') {
                let root = alias_root(fd, &candidate.interface)?;
                candidate.interface = root.clone();
                if candidate.address.ip.is_ipv6() {
                    candidate.address.scope_name = (candidate.address.scope != 0).then_some(root);
                }
            }
            Some(candidate)
        })
        .collect()
}

fn add_candidate(interfaces: &mut Vec<InterfaceAddresses>, candidate: Candidate) {
    let index = interfaces
        .iter()
        .position(|interface| interface.name == candidate.interface)
        .unwrap_or_else(|| {
            interfaces.insert(
                0,
                InterfaceAddresses {
                    name: candidate.interface.clone(),
                    ..InterfaceAddresses::default()
                },
            );
            0
        });
    let interface = &mut interfaces[index];
    if candidate.address.ip.is_ipv4() {
        interface.ipv4.insert(0, candidate.address);
    } else {
        interface.ipv6.insert(0, candidate.address);
    }
}

fn java_candidate_order(ipv4: Vec<Candidate>, ipv6: Vec<Candidate>) -> Vec<InterfaceAddresses> {
    let mut interfaces = Vec::new();
    for candidate in ipv4.into_iter().chain(ipv6) {
        add_candidate(&mut interfaces, candidate);
    }
    interfaces
}

pub(super) fn interfaces(prefer_ipv4_stack: bool) -> io::Result<Vec<Address>> {
    let socket = open_ipv4_socket()?;
    let ipv4 = ipv4_table(socket.as_raw_fd())?;
    let ipv6 = if prefer_ipv4_stack {
        Vec::new()
    } else {
        ipv6_table(socket.as_raw_fd())
    };
    let mut candidates = Vec::new();
    for interface in java_candidate_order(ipv4, ipv6) {
        let bridge = Path::new("/sys/class/net")
            .join(&interface.name)
            .join("bridge")
            .exists();
        if bridge {
            continue;
        }
        let flags = get_flags(socket.as_raw_fd(), &interface.name)?;
        if !eligible(&interface.name, flags, false) {
            continue;
        }
        candidates.extend(interface.ipv6);
        candidates.extend(interface.ipv4);
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(interface: &str, address: &str) -> Candidate {
        Candidate {
            interface: interface.to_owned(),
            address: Address {
                ip: address.parse().unwrap(),
                scope: 0,
                scope_name: None,
            },
        }
    }

    #[test]
    fn linux_ifconf_abi_matches_linux_uapi_layout() {
        let (ifreq_size, ifconf_size) = if cfg!(target_pointer_width = "64") {
            (40, 16)
        } else {
            (32, 8)
        };
        assert_eq!(size_of::<IfReq>(), ifreq_size);
        assert_eq!(size_of::<IfConf>(), ifconf_size);
    }

    #[test]
    fn proc_ipv6_row_keeps_linux_interface_scope_for_all_address_kinds() {
        let row = parse_ipv6_row("20010db8000000000000000000000001 02 40 00 80 eth0").unwrap();
        assert_eq!(row.interface, "eth0");
        assert_eq!(row.address.ip, "2001:db8::1".parse::<IpAddr>().unwrap());
        assert_eq!(row.address.scope, 2);
        assert_eq!(row.address.scope_name.as_deref(), Some("eth0"));
        assert!(parse_ipv6_row("2001 02 40 00 80 eth0").is_none());
    }

    #[test]
    fn linux_loopback_proc_scope_matches_invoked_java_551() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/address-linux.json"
        ))
        .unwrap();
        let cases = fixture["linuxIpv6LoopbackCandidates"].as_array().unwrap();
        assert!(
            !cases.is_empty(),
            "pinned Linux JDK did not expose IPv6 loopback"
        );
        for case in cases {
            let interface = case["procEntryInterface"].as_str().unwrap();
            let scope = case["scopeId"].as_u64().unwrap() as u32;
            let row = format!(
                "{} {:02x} 00 00 00 {}",
                case["procEntryAddress"].as_str().unwrap(),
                scope,
                interface
            );
            let candidate = parse_ipv6_row(&row).unwrap();
            assert_eq!(candidate.interface, interface);
            assert_eq!(
                candidate.address.ip,
                case["ip"].as_str().unwrap().parse::<IpAddr>().unwrap()
            );
            assert_eq!(
                candidate.address.scope,
                case["scopeId"].as_u64().unwrap() as u32
            );
            assert_eq!(
                candidate.address.scope_name.as_deref(),
                Some(case["interfaceName"].as_str().unwrap())
            );
            assert_eq!(
                super::super::normalize(&candidate.address),
                case["normalized"].as_str().unwrap()
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_production_enumerator_matches_invoked_java_loopback_scope() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/address-linux.json"
        ))
        .unwrap();
        let actual = super::super::interfaces(false).unwrap();
        for case in fixture["linuxIpv6LoopbackCandidates"].as_array().unwrap() {
            let expected = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scopeId"].as_u64().unwrap() as u32,
                scope_name: Some(case["interfaceName"].as_str().unwrap().to_owned()),
            };
            assert!(
                actual.contains(&expected),
                "Java Linux loopback candidate is missing from the production enumerator: {case}; {actual:?}"
            );
            assert_eq!(
                super::super::normalize(&expected),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run in Linux after capturing a Java 5.5.1 candidate snapshot"]
    fn linux_production_candidate_order_matches_invoked_java_runtime_snapshot() {
        let path = std::env::var_os("ROCKETMQ_JAVA_LINUX_CANDIDATES")
            .expect("set ROCKETMQ_JAVA_LINUX_CANDIDATES to the Java probe output path");
        let fixture: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let expected = fixture["linuxCandidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|candidate| candidate["normalized"].as_str().unwrap())
            .collect::<Vec<_>>();
        let actual = interfaces(false)
            .unwrap()
            .iter()
            .map(super::super::normalize)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "Linux interface candidate order");
    }

    #[test]
    fn candidate_lists_match_openjdk_table_and_head_insertion_order() {
        let ordered = java_candidate_order(
            vec![
                candidate("eth0", "10.0.0.1"),
                candidate("eth0", "10.0.0.2"),
                candidate("wlan0", "192.0.2.1"),
            ],
            vec![
                candidate("eth0", "fe80::1"),
                candidate("v6only0", "2001:db8::1"),
                candidate("eth0", "fe80::2"),
            ],
        );
        let flattened = ordered
            .into_iter()
            .flat_map(|interface| {
                std::iter::once(interface.name).chain(
                    interface
                        .ipv6
                        .into_iter()
                        .chain(interface.ipv4)
                        .map(|address| address.ip.to_string()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            flattened,
            [
                "v6only0",
                "2001:db8::1",
                "wlan0",
                "192.0.2.1",
                "eth0",
                "fe80::2",
                "fe80::1",
                "10.0.0.2",
                "10.0.0.1",
            ]
        );
    }
}
