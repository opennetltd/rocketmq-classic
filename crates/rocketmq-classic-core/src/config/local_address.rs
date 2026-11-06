use std::{
    io,
    net::{IpAddr, ToSocketAddrs},
    sync::OnceLock,
};

#[cfg(any(test, windows, all(unix, not(target_os = "linux"))))]
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Address {
    ip: IpAddr,
    scope: u32,
    scope_name: Option<String>,
}

fn internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            matches!(octets[0], 10 | 127)
                || (octets[0] == 172 && (16..=31).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 168)
        }
        IpAddr::V6(ip) => {
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.segments()[0] & 0xffc0 == 0xfe80
                || ip.segments()[0] & 0xffc0 == 0xfec0
        }
    }
}

fn normalize(address: &Address) -> String {
    match address.ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => {
            let host = ip
                .segments()
                .iter()
                .map(|part| format!("{part:x}"))
                .collect::<Vec<_>>()
                .join(":");
            if let Some(name) = &address.scope_name {
                format!("[{host}%{name}]")
            } else if address.scope == 0 {
                format!("[{host}]")
            } else {
                format!("[{host}%{}]", address.scope)
            }
        }
    }
}

fn select(addresses: Vec<Address>) -> Option<Address> {
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for address in addresses
        .into_iter()
        .filter(|address| !address.ip.is_loopback())
    {
        if address.ip.is_ipv4() {
            v4.push(address);
        } else {
            v6.push(address);
        }
    }
    if !v4.is_empty() {
        v4.iter()
            .find(|address| !internal(address.ip))
            .cloned()
            .or_else(|| v4.pop())
    } else {
        v6.iter()
            .find(|address| !internal(address.ip))
            .cloned()
            .or_else(|| v6.into_iter().next())
    }
}

#[cfg(any(windows, test))]
fn java_windows_interface_is_eligible(
    admin_up: bool,
    operational_up: bool,
    point_to_point: bool,
) -> bool {
    admin_up && operational_up && !point_to_point
}

#[cfg(any(windows, test))]
fn java_windows_unicast_address_is_usable(dad_state: i32) -> bool {
    // Windows NL_DAD_STATE values are 3 for deprecated and 4 for preferred.
    matches!(dad_state, 3 | 4)
}

#[cfg(any(windows, test))]
fn java_windows_interface_candidate_order(
    mut unicast: Vec<Address>,
    mut anycast: Vec<Address>,
) -> Vec<Address> {
    // OpenJDK prepends each table row to its native address list, so the
    // resulting enumeration is reverse table order, with anycast before
    // unicast because its rows are prepended second.
    unicast.reverse();
    anycast.reverse();
    anycast.extend(unicast);
    anycast
}

#[cfg(any(windows, test))]
// The caller supplies IP Helper rows in native table order and snapshots the
// two GetIfEntry2 predicates that Java queries separately.
struct WindowsInterfaceRecord {
    luid: u64,
    name: String,
    admin_up: bool,
    operational_up: bool,
    point_to_point: bool,
}

#[cfg(any(windows, test))]
struct WindowsUnicastRecord {
    luid: u64,
    dad_state: i32,
    address: windows_sys::Win32::Networking::WinSock::SOCKADDR_INET,
}

#[cfg(any(windows, test))]
struct WindowsAnycastRecord {
    luid: u64,
    address: windows_sys::Win32::Networking::WinSock::SOCKADDR_INET,
}

#[cfg(any(windows, test))]
fn windows_interface_candidates(
    interfaces: &[WindowsInterfaceRecord],
    unicast: &[WindowsUnicastRecord],
    anycast: &[WindowsAnycastRecord],
    prefer_ipv4_stack: bool,
) -> Vec<Address> {
    let mut addresses = Vec::new();
    for interface in interfaces {
        if !java_windows_interface_is_eligible(
            interface.admin_up,
            interface.operational_up,
            interface.point_to_point,
        ) {
            continue;
        }

        let interface_unicast = unicast
            .iter()
            .filter(|row| {
                row.luid == interface.luid && java_windows_unicast_address_is_usable(row.dad_state)
            })
            .filter_map(|row| windows_address(row.address, &interface.name))
            .filter(|address| !prefer_ipv4_stack || address.ip.is_ipv4())
            .collect::<Vec<_>>();
        let interface_anycast = anycast
            .iter()
            .filter(|row| row.luid == interface.luid)
            .filter_map(|row| windows_address(row.address, &interface.name))
            .filter(|address| !prefer_ipv4_stack || address.ip.is_ipv4())
            .collect::<Vec<_>>();
        addresses.extend(java_windows_interface_candidate_order(
            interface_unicast,
            interface_anycast,
        ));
    }
    addresses
}

#[cfg(any(target_os = "linux", test))]
fn java_linux_ipv6_scope(_native_scope: u32, interface_index: u32) -> u32 {
    // OpenJDK's Linux enumerator sets sin6_scope_id from /proc/net/if_inet6's
    // interface index for every IPv6 address. glibc getifaddrs only supplies
    // a scope for link-local and link-scoped multicast addresses.
    interface_index
}

pub(super) fn get() -> io::Result<String> {
    let prefer_ipv4_stack = java_prefer_ipv4_stack();
    resolve(interfaces(prefer_ipv4_stack), || {
        local_host(prefer_ipv4_stack)
    })
}

fn java_prefer_ipv4_stack_property(value: Option<String>) -> bool {
    super::java_boolean_property(value, false)
}

fn java_prefer_ipv4_stack() -> bool {
    static PREFER_IPV4_STACK: OnceLock<bool> = OnceLock::new();
    *PREFER_IPV4_STACK.get_or_init(|| {
        java_prefer_ipv4_stack_property(std::env::var("java.net.preferIPv4Stack").ok())
    })
}

fn resolve(
    addresses: io::Result<Vec<Address>>,
    fallback: impl FnOnce() -> io::Result<Address>,
) -> io::Result<String> {
    if let Some(address) = select(addresses?) {
        return Ok(normalize(&address));
    }
    fallback().map(|address| normalize(&address))
}

fn local_host(prefer_ipv4_stack: bool) -> io::Result<Address> {
    // Java InetAddress.getLocalHost resolves the OS host name, not a hardcoded
    // loopback or a UDP route probe to an unrelated Internet destination.
    let host = hostname::get()?
        .into_string()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-Unicode local host name"))?;
    let socket = (host.as_str(), 0)
        .to_socket_addrs()?
        .find(|socket| !prefer_ipv4_stack || socket.is_ipv4())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "local host has no addresses",
            )
        })?;
    let scope = match socket {
        std::net::SocketAddr::V6(socket) => socket.scope_id(),
        _ => 0,
    };
    Ok(Address {
        ip: socket.ip(),
        scope,
        scope_name: None,
    })
}

#[cfg(unix)]
fn eligible(_name: &str, flags: u32, bridge: bool) -> bool {
    flags & libc::IFF_UP as u32 != 0
        && flags & libc::IFF_RUNNING as u32 != 0
        && flags & libc::IFF_POINTOPOINT as u32 == 0
        && !bridge
}

#[cfg(any(unix, test))]
fn java_unix_interface_root_name(name: &str, alias_parent_accessible: bool) -> Option<&str> {
    match name.split_once(':') {
        Some((parent, _)) if !parent.is_empty() && alias_parent_accessible => Some(parent),
        Some(_) => None,
        None => Some(name),
    }
}

#[cfg(any(
    test,
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "macos"
))]
fn java_bsd_ipv6_scope(_native_scope: u32, interface_index: u32) -> u32 {
    // OpenJDK's BSD enumerator replaces the native scope on every IPv6
    // candidate with the interface index. Its failed lookup is -1, whose
    // unsigned bit pattern is u32::MAX here so the named scope is retained.
    if interface_index == 0 {
        u32::MAX
    } else {
        interface_index
    }
}

#[cfg(any(
    test,
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "macos"
))]
#[derive(Debug)]
struct UnixCandidate {
    interface: String,
    address: Address,
}

#[cfg(any(
    test,
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "macos"
))]
#[derive(Default)]
struct UnixInterfaceAddresses {
    name: String,
    ipv4: Vec<Address>,
    ipv6: Vec<Address>,
}

#[cfg(any(
    test,
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "macos"
))]
fn add_unix_candidate(interfaces: &mut Vec<UnixInterfaceAddresses>, candidate: UnixCandidate) {
    let index = interfaces
        .iter()
        .position(|interface| interface.name == candidate.interface)
        .unwrap_or_else(|| {
            interfaces.insert(
                0,
                UnixInterfaceAddresses {
                    name: candidate.interface.clone(),
                    ..UnixInterfaceAddresses::default()
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

#[cfg(any(
    test,
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "macos"
))]
fn java_unix_candidate_order(ipv4: Vec<UnixCandidate>, ipv6: Vec<UnixCandidate>) -> Vec<Address> {
    let mut interfaces = Vec::new();
    for candidate in ipv4.into_iter().chain(ipv6) {
        add_unix_candidate(&mut interfaces, candidate);
    }
    interfaces
        .into_iter()
        .flat_map(|interface| interface.ipv6.into_iter().chain(interface.ipv4))
        .collect()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn interfaces(prefer_ipv4_stack: bool) -> io::Result<Vec<Address>> {
    use std::{
        ffi::{CStr, CString},
        ptr,
    };
    let mut first = ptr::null_mut();
    // SAFETY: getifaddrs writes an owned linked list; the guard releases it once.
    if unsafe { libc::getifaddrs(&mut first) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct List(*mut libc::ifaddrs);
    impl Drop for List {
        fn drop(&mut self) {
            // SAFETY: this guard owns exactly the list returned by getifaddrs.
            unsafe {
                libc::freeifaddrs(self.0);
            }
        }
    }
    let _guard = List(first);
    let mut current = first;
    #[cfg(not(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos"
    )))]
    let mut addresses = Vec::new();
    #[cfg(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos"
    ))]
    let mut ipv4_candidates = Vec::new();
    #[cfg(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos"
    ))]
    let mut ipv6_candidates = Vec::new();
    while !current.is_null() {
        // SAFETY: nodes/names/sockaddrs live until the list guard is dropped.
        let interface = unsafe { &*current };
        current = interface.ifa_next;
        if interface.ifa_addr.is_null() || interface.ifa_name.is_null() {
            continue;
        }
        let native_name = unsafe { CStr::from_ptr(interface.ifa_name) }
            .to_string_lossy()
            .into_owned();
        let alias_parent_accessible = if let Some((parent, _)) = native_name.split_once(':') {
            let parent = CString::new(parent)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            // Java checks the parent with getFlags before attaching a logical
            // alias address to its physical NetworkInterface.
            (unsafe { libc::if_nametoindex(parent.as_ptr()) }) != 0
        } else {
            true
        };
        let Some(name) = java_unix_interface_root_name(&native_name, alias_parent_accessible)
        else {
            continue;
        };
        let flags = interface.ifa_flags;
        let bridge = cfg!(target_os = "linux")
            && std::path::Path::new("/sys/class/net")
                .join(name)
                .join("bridge")
                .exists();
        if !eligible(name, flags, bridge) {
            continue;
        }
        let family = unsafe { (*interface.ifa_addr).sa_family } as i32;
        if prefer_ipv4_stack && family == libc::AF_INET6 {
            continue;
        }
        let address = match family {
            libc::AF_INET => {
                let socket = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in) };
                Address {
                    ip: IpAddr::V4(Ipv4Addr::from(socket.sin_addr.s_addr.to_ne_bytes())),
                    scope: 0,
                    scope_name: None,
                }
            }
            libc::AF_INET6 => {
                let socket = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in6) };
                #[cfg(target_os = "linux")]
                let scope = {
                    let name = CString::new(name)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    java_linux_ipv6_scope(socket.sin6_scope_id, unsafe {
                        libc::if_nametoindex(name.as_ptr())
                    })
                };
                #[cfg(any(
                    target_os = "freebsd",
                    target_os = "openbsd",
                    target_os = "netbsd",
                    target_os = "dragonfly",
                    target_os = "macos"
                ))]
                let scope = {
                    let name = CString::new(name)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    java_bsd_ipv6_scope(socket.sin6_scope_id, unsafe {
                        libc::if_nametoindex(name.as_ptr())
                    })
                };
                #[cfg(not(target_os = "linux"))]
                #[cfg(not(any(
                    target_os = "freebsd",
                    target_os = "openbsd",
                    target_os = "netbsd",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                let scope = socket.sin6_scope_id;
                Address {
                    ip: IpAddr::V6(Ipv6Addr::from(socket.sin6_addr.s6_addr)),
                    scope,
                    // Java's Inet6Address stores the NetworkInterface itself
                    // for scoped addresses, so getHostAddress prints its name.
                    scope_name: (scope != 0).then(|| name.to_owned()),
                }
            }
            _ => continue,
        };
        #[cfg(any(
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
            target_os = "macos"
        ))]
        {
            let candidate = UnixCandidate {
                interface: name.to_owned(),
                address,
            };
            if family == libc::AF_INET {
                ipv4_candidates.push(candidate);
            } else {
                ipv6_candidates.push(candidate);
            }
        }
        #[cfg(not(any(
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "dragonfly",
            target_os = "macos"
        )))]
        addresses.push(address);
    }
    #[cfg(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos"
    ))]
    return Ok(java_unix_candidate_order(ipv4_candidates, ipv6_candidates));
    #[cfg(not(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "macos"
    )))]
    Ok(addresses)
}

#[cfg(target_os = "linux")]
fn interfaces(prefer_ipv4_stack: bool) -> io::Result<Vec<Address>> {
    linux_interfaces::interfaces(prefer_ipv4_stack)
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[path = "local_address_linux.rs"]
mod linux_interfaces;

#[cfg(windows)]
fn interfaces(prefer_ipv4_stack: bool) -> io::Result<Vec<Address>> {
    use windows_sys::Win32::{
        NetworkManagement::{
            IpHelper::{
                ConvertInterfaceLuidToNameW, FreeMibTable, GetAnycastIpAddressTable, GetIfEntry2,
                GetIfTable2, GetUnicastIpAddressTable, MIB_ANYCASTIPADDRESS_TABLE, MIB_IF_ROW2,
                MIB_IF_TABLE2, MIB_UNICASTIPADDRESS_TABLE,
            },
            Ndis::{
                IfOperStatusUp, NET_IF_ACCESS_POINT_TO_POINT, NET_IF_ADMIN_STATUS_UP, NET_LUID_LH,
            },
        },
        Networking::WinSock::AF_UNSPEC,
    };

    struct MibTable(*const std::ffi::c_void);
    impl Drop for MibTable {
        fn drop(&mut self) {
            // SAFETY: each pointer is returned by an IP Helper table API and
            // remains owned by this guard until FreeMibTable is called.
            unsafe { FreeMibTable(self.0) };
        }
    }

    fn interface_name(luid: &NET_LUID_LH) -> io::Result<String> {
        // OpenJDK obtains NetworkInterface.getName() from this LUID conversion.
        let mut name = [0u16; 257];
        let result = unsafe {
            ConvertInterfaceLuidToNameW(std::ptr::from_ref(luid), name.as_mut_ptr(), name.len())
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let length = name
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(name.len());
        String::from_utf16(&name[..length])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn luid_value(luid: &NET_LUID_LH) -> u64 {
        // SAFETY: the IP Helper APIs initialize the canonical 64-bit view of
        // every NET_LUID_LH value returned in these tables.
        unsafe { luid.Value }
    }

    fn interface_row(index: u32) -> io::Result<MIB_IF_ROW2> {
        let mut row = MIB_IF_ROW2 {
            InterfaceIndex: index,
            ..Default::default()
        };
        let result = unsafe { GetIfEntry2(&mut row) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(row)
    }

    let mut interfaces = std::ptr::null_mut::<MIB_IF_TABLE2>();
    let result = unsafe { GetIfTable2(&mut interfaces) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if interfaces.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetIfTable2 returned a null table",
        ));
    }
    let _interfaces_guard = MibTable(interfaces.cast());

    let mut unicast = std::ptr::null_mut::<MIB_UNICASTIPADDRESS_TABLE>();
    let result = unsafe { GetUnicastIpAddressTable(AF_UNSPEC, &mut unicast) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if unicast.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetUnicastIpAddressTable returned a null table",
        ));
    }
    let _unicast_guard = MibTable(unicast.cast());

    let mut anycast = std::ptr::null_mut::<MIB_ANYCASTIPADDRESS_TABLE>();
    let result = unsafe { GetAnycastIpAddressTable(AF_UNSPEC, &mut anycast) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if anycast.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetAnycastIpAddressTable returned a null table",
        ));
    }
    let _anycast_guard = MibTable(anycast.cast());

    // SAFETY: the IP Helper APIs allocate each flexible table with at least
    // `NumEntries` initialized rows, and all guards outlive these slices.
    let interfaces = unsafe {
        std::slice::from_raw_parts(
            (*interfaces).Table.as_ptr(),
            (*interfaces).NumEntries as usize,
        )
    };
    let unicast = unsafe {
        std::slice::from_raw_parts((*unicast).Table.as_ptr(), (*unicast).NumEntries as usize)
    };
    let anycast = unsafe {
        std::slice::from_raw_parts((*anycast).Table.as_ptr(), (*anycast).NumEntries as usize)
    };

    let interfaces = interfaces
        .iter()
        .map(|interface| {
            let name = interface_name(&interface.InterfaceLuid)?;
            // NetworkInterface.isUp() and isPointToPoint() each query the
            // current interface row after Java has enumerated the interface.
            let up_state = interface_row(interface.InterfaceIndex)?;
            let point_to_point_state = interface_row(interface.InterfaceIndex)?;
            Ok(WindowsInterfaceRecord {
                luid: luid_value(&interface.InterfaceLuid),
                name,
                admin_up: up_state.AdminStatus == NET_IF_ADMIN_STATUS_UP,
                operational_up: up_state.OperStatus == IfOperStatusUp,
                point_to_point: point_to_point_state.AccessType == NET_IF_ACCESS_POINT_TO_POINT,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let unicast = unicast
        .iter()
        .map(|row| WindowsUnicastRecord {
            luid: luid_value(&row.InterfaceLuid),
            dad_state: row.DadState,
            address: row.Address,
        })
        .collect::<Vec<_>>();
    let anycast = anycast
        .iter()
        .map(|row| WindowsAnycastRecord {
            luid: luid_value(&row.InterfaceLuid),
            address: row.Address,
        })
        .collect::<Vec<_>>();
    Ok(windows_interface_candidates(
        &interfaces,
        &unicast,
        &anycast,
        prefer_ipv4_stack,
    ))
}

#[cfg(any(windows, test))]
fn windows_address(
    address: windows_sys::Win32::Networking::WinSock::SOCKADDR_INET,
    interface_name: &str,
) -> Option<Address> {
    use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};

    // SAFETY: `si_family` is the active discriminator for this union.
    match unsafe { address.si_family } {
        AF_INET => {
            // SAFETY: AF_INET identifies the IPv4 member of SOCKADDR_INET.
            let socket = unsafe { address.Ipv4 };
            Some(Address {
                ip: IpAddr::V4(Ipv4Addr::from(
                    unsafe { socket.sin_addr.S_un.S_addr }.to_ne_bytes(),
                )),
                scope: 0,
                scope_name: None,
            })
        }
        AF_INET6 => {
            // SAFETY: AF_INET6 identifies the IPv6 member of SOCKADDR_INET.
            let socket = unsafe { address.Ipv6 };
            let scope = unsafe { socket.Anonymous.sin6_scope_id };
            Some(Address {
                ip: IpAddr::V6(Ipv6Addr::from(unsafe { socket.sin6_addr.u.Byte })),
                scope,
                scope_name: (scope != 0).then(|| interface_name.to_owned()),
            })
        }
        _ => None,
    }
}

#[cfg(not(any(unix, windows)))]
fn interfaces(_prefer_ipv4_stack: bool) -> io::Result<Vec<Address>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native interface enumeration unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_sockaddr_conversion_preserves_address_bytes_and_scope() {
        use windows_sys::Win32::Networking::WinSock::{
            AF_INET, AF_INET6, IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0, SOCKADDR_IN, SOCKADDR_IN6,
            SOCKADDR_IN6_0, SOCKADDR_INET,
        };

        let ipv4_octets = [192, 0, 2, 17];
        let ipv4 = SOCKADDR_INET {
            Ipv4: SOCKADDR_IN {
                sin_family: AF_INET,
                sin_addr: IN_ADDR {
                    S_un: IN_ADDR_0 {
                        S_addr: u32::from_ne_bytes(ipv4_octets),
                    },
                },
                ..Default::default()
            },
        };
        assert_eq!(
            windows_address(ipv4, "Ethernet 3"),
            Some(Address {
                ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 17)),
                scope: 0,
                scope_name: None,
            })
        );

        let ipv6_octets = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9];
        let ipv6_socket = SOCKADDR_IN6 {
            sin6_family: AF_INET6,
            sin6_addr: IN6_ADDR {
                u: IN6_ADDR_0 { Byte: ipv6_octets },
            },
            Anonymous: SOCKADDR_IN6_0 { sin6_scope_id: 11 },
            ..Default::default()
        };
        let ipv6 = SOCKADDR_INET { Ipv6: ipv6_socket };
        assert_eq!(
            windows_address(ipv6, "Ethernet 3"),
            Some(Address {
                ip: IpAddr::V6("2001:db8::9".parse().unwrap()),
                scope: 11,
                scope_name: Some("Ethernet 3".to_owned()),
            })
        );

        let ipv6_without_scope = SOCKADDR_INET {
            Ipv6: SOCKADDR_IN6 {
                Anonymous: SOCKADDR_IN6_0 { sin6_scope_id: 0 },
                ..ipv6_socket
            },
        };
        assert_eq!(
            windows_address(ipv6_without_scope, "Ethernet 3"),
            Some(Address {
                ip: IpAddr::V6("2001:db8::9".parse().unwrap()),
                scope: 0,
                scope_name: None,
            })
        );

        assert_eq!(
            windows_address(SOCKADDR_INET::default(), "Ethernet 3"),
            None
        );
    }

    #[test]
    fn windows_interface_candidate_pipeline_joins_filters_and_orders_tables() {
        use windows_sys::Win32::Networking::WinSock::{
            AF_INET, AF_INET6, IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0, SOCKADDR_IN, SOCKADDR_IN6,
            SOCKADDR_IN6_0, SOCKADDR_INET,
        };

        fn ipv4(octets: [u8; 4]) -> SOCKADDR_INET {
            SOCKADDR_INET {
                Ipv4: SOCKADDR_IN {
                    sin_family: AF_INET,
                    sin_addr: IN_ADDR {
                        S_un: IN_ADDR_0 {
                            S_addr: u32::from_ne_bytes(octets),
                        },
                    },
                    ..Default::default()
                },
            }
        }

        fn ipv6(octets: [u8; 16], scope: u32) -> SOCKADDR_INET {
            SOCKADDR_INET {
                Ipv6: SOCKADDR_IN6 {
                    sin6_family: AF_INET6,
                    sin6_addr: IN6_ADDR {
                        u: IN6_ADDR_0 { Byte: octets },
                    },
                    Anonymous: SOCKADDR_IN6_0 {
                        sin6_scope_id: scope,
                    },
                    ..Default::default()
                },
            }
        }

        let interfaces = [
            WindowsInterfaceRecord {
                luid: 10,
                name: "Ethernet 0".into(),
                admin_up: true,
                operational_up: true,
                point_to_point: false,
            },
            WindowsInterfaceRecord {
                luid: 20,
                name: "Down".into(),
                admin_up: false,
                operational_up: true,
                point_to_point: false,
            },
            WindowsInterfaceRecord {
                luid: 30,
                name: "Point-to-point".into(),
                admin_up: true,
                operational_up: true,
                point_to_point: true,
            },
            WindowsInterfaceRecord {
                luid: 40,
                name: "Ethernet 1".into(),
                admin_up: true,
                operational_up: true,
                point_to_point: false,
            },
        ];
        let unicast = [
            WindowsUnicastRecord {
                luid: 10,
                dad_state: 4,
                address: ipv4([10, 0, 0, 1]),
            },
            WindowsUnicastRecord {
                luid: 10,
                dad_state: 2,
                address: ipv4([10, 0, 0, 99]),
            },
            WindowsUnicastRecord {
                luid: 10,
                dad_state: 3,
                address: ipv4([10, 0, 0, 2]),
            },
            WindowsUnicastRecord {
                luid: 10,
                dad_state: 4,
                address: ipv6([0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 12),
            },
            WindowsUnicastRecord {
                luid: 20,
                dad_state: 4,
                address: ipv4([192, 0, 2, 20]),
            },
            WindowsUnicastRecord {
                luid: 40,
                dad_state: 4,
                address: ipv6(
                    [0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x40],
                    0,
                ),
            },
        ];
        let anycast = [
            WindowsAnycastRecord {
                luid: 10,
                address: ipv4([198, 51, 100, 1]),
            },
            WindowsAnycastRecord {
                luid: 10,
                address: ipv4([198, 51, 100, 2]),
            },
            WindowsAnycastRecord {
                luid: 20,
                address: ipv4([198, 51, 100, 20]),
            },
            WindowsAnycastRecord {
                luid: 99,
                address: ipv4([203, 0, 113, 99]),
            },
        ];

        let render = |prefer_ipv4_stack| {
            windows_interface_candidates(&interfaces, &unicast, &anycast, prefer_ipv4_stack)
                .iter()
                .map(normalize)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            render(false),
            [
                "198.51.100.2",
                "198.51.100.1",
                "[fe80:0:0:0:0:0:0:1%Ethernet 0]",
                "10.0.0.2",
                "10.0.0.1",
                "[2001:db8:0:0:0:0:0:40]",
            ]
        );
        assert_eq!(
            render(true),
            ["198.51.100.2", "198.51.100.1", "10.0.0.2", "10.0.0.1"]
        );
    }

    #[test]
    fn windows_interface_filter_matches_java_admin_oper_and_access_checks() {
        assert!(java_windows_interface_is_eligible(true, true, false));
        assert!(!java_windows_interface_is_eligible(false, true, false));
        assert!(!java_windows_interface_is_eligible(true, false, false));
        assert!(!java_windows_interface_is_eligible(true, true, true));
    }
    #[test]
    fn windows_unicast_address_state_matches_java_preferred_and_deprecated_filter() {
        assert!(!java_windows_unicast_address_is_usable(0)); // invalid
        assert!(!java_windows_unicast_address_is_usable(1)); // tentative
        assert!(!java_windows_unicast_address_is_usable(2)); // duplicate
        assert!(java_windows_unicast_address_is_usable(3)); // deprecated
        assert!(java_windows_unicast_address_is_usable(4)); // preferred
    }
    #[test]
    fn windows_candidate_order_matches_java_native_address_list() {
        assert_eq!(
            java_windows_interface_candidate_order(
                vec![
                    address("10.0.0.1"),
                    address("10.0.0.2"),
                    address("10.0.0.3"),
                ],
                vec![address("192.0.2.1"), address("192.0.2.2")],
            ),
            vec![
                address("192.0.2.2"),
                address("192.0.2.1"),
                address("10.0.0.3"),
                address("10.0.0.2"),
                address("10.0.0.1"),
            ]
        );
    }
    #[cfg(unix)]
    #[test]
    fn interface_filter_matches_java_up_running_and_exclusions() {
        assert!(eligible(
            "veth0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            false
        ));
        assert!(eligible(
            "eth0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            false
        ));
        assert!(!eligible("eth0", libc::IFF_UP as u32, false));
        assert!(!eligible("eth0", libc::IFF_RUNNING as u32, false));
        assert!(!eligible(
            "br0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            true
        ));
        assert!(!eligible(
            "ppp0",
            (libc::IFF_UP | libc::IFF_RUNNING | libc::IFF_POINTOPOINT) as u32,
            false
        ));
    }
    #[test]
    fn unix_logical_alias_matches_java_parent_and_virtual_fallback() {
        assert_eq!(java_unix_interface_root_name("eth0:1", true), Some("eth0"));
        assert_eq!(java_unix_interface_root_name("eth0:1", false), None);
        assert_eq!(java_unix_interface_root_name("eth0", false), Some("eth0"));
        assert_eq!(java_unix_interface_root_name(":1", true), None);
    }
    #[test]
    fn prefer_ipv4_stack_property_uses_java_boolean_rules() {
        assert!(java_prefer_ipv4_stack_property(Some("true".to_owned())));
        assert!(java_prefer_ipv4_stack_property(Some("TrUe".to_owned())));
        assert!(!java_prefer_ipv4_stack_property(Some(" true ".to_owned())));
        assert!(!java_prefer_ipv4_stack_property(Some("false".to_owned())));
        assert!(!java_prefer_ipv4_stack_property(None));
    }
    #[test]
    fn prefer_ipv4_stack_process_property_is_snapshotted() {
        const CHILD_MARKER: &str = "ROCKETMQ_P6_IPV4_STACK_TEST_CHILD";
        if std::env::var_os(CHILD_MARKER).is_some() {
            assert!(java_prefer_ipv4_stack());
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "config::local_address::tests::prefer_ipv4_stack_process_property_is_snapshotted",
                "--nocapture",
            ])
            .env(CHILD_MARKER, "1")
            .env("java.net.preferIPv4Stack", "TRUE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout)
        );
    }
    #[test]
    fn bsd_ipv6_scope_uses_interface_index() {
        assert_eq!(java_bsd_ipv6_scope(0, 7), 7);
        assert_eq!(java_bsd_ipv6_scope(3, 11), 11);
        assert_eq!(java_bsd_ipv6_scope(5, 0), u32::MAX);
    }
    #[test]
    fn bsd_candidate_order_matches_family_passes_and_java_head_insertion() {
        let candidate = |interface: &str, ip: &str| UnixCandidate {
            interface: interface.to_owned(),
            address: address(ip),
        };
        assert_eq!(
            java_unix_candidate_order(
                vec![
                    candidate("en0", "10.0.0.1"),
                    candidate("en0", "10.0.0.2"),
                    candidate("wlan0", "192.0.2.1"),
                ],
                vec![
                    candidate("en0", "fe80::1"),
                    candidate("v6only0", "2001:db8::1"),
                    candidate("en0", "fe80::2"),
                ],
            ),
            vec![
                address("2001:db8::1"),
                address("192.0.2.1"),
                address("fe80::2"),
                address("fe80::1"),
                address("10.0.0.2"),
                address("10.0.0.1"),
            ]
        );
    }
    #[test]
    fn fallback_only_runs_for_successful_empty_or_loopback_only_enumeration() {
        assert_eq!(
            resolve(Ok(vec![address("127.0.0.1"), address("::1")]), || Ok(
                address("::1")
            ))
            .unwrap(),
            "[0:0:0:0:0:0:0:1]"
        );
        assert_eq!(
            resolve(Ok(vec![address("10.0.0.1")]), || panic!(
                "must not resolve hostname"
            ))
            .unwrap(),
            "10.0.0.1"
        );
        let error = resolve(
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            || panic!("enumeration failed"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let error = resolve(Ok(vec![]), || {
            Err(io::Error::from(io::ErrorKind::AddrNotAvailable))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
    }
    #[test]
    fn address_helpers_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        for case in fixture["addresses"].as_array().unwrap() {
            let address = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: None,
            };
            assert_eq!(
                internal(address.ip),
                case["internal"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                address.ip.is_loopback(),
                case["loopback"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                normalize(&address),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
        for case in fixture["scopeCases"].as_array().unwrap() {
            let address = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: Some(case["interfaceName"].as_str().unwrap().to_owned()),
            };
            assert_eq!(
                address.scope_name.as_deref(),
                case["interfaceName"].as_str()
            );
            assert_eq!(
                normalize(&address),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_interface_enumeration_retains_java_named_ipv6_scope() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let addresses = interfaces(false).unwrap();
        for case in fixture["scopeCases"].as_array().unwrap() {
            let expected = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: Some(case["interfaceName"].as_str().unwrap().to_owned()),
            };
            assert!(
                addresses.contains(&expected),
                "Java interface scope missing from native enumeration: {case}; {addresses:?}"
            );
            assert_eq!(
                normalize(&expected),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn prefer_ipv4_stack_excludes_ipv6_interface_candidates() {
        assert!(
            interfaces(true)
                .unwrap()
                .into_iter()
                .all(|address| address.ip.is_ipv4())
        );
    }
    fn address(ip: &str) -> Address {
        Address {
            ip: ip.parse().unwrap(),
            scope: 0,
            scope_name: None,
        }
    }
    #[test]
    fn java_selection_matches_java_family_and_candidate_order() {
        assert_eq!(
            select(vec![
                address("::1"),
                address("2001:db8::1"),
                address("10.1.1.1"),
                address("192.0.2.1"),
                address("192.0.2.2")
            ]),
            Some(address("192.0.2.1"))
        );
        assert_eq!(
            select(vec![
                address("10.1.1.1"),
                address("2001:db8::1"),
                address("192.168.1.2")
            ]),
            Some(address("192.168.1.2"))
        );
        assert_eq!(
            select(vec![
                address("fe80::1"),
                address("fec0::1"),
                address("fc00::1"),
                address("2001:db8::2")
            ]),
            Some(address("fc00::1"))
        );
        assert_eq!(
            select(vec![address("fe80::1"), address("fec0::1")]),
            Some(address("fe80::1"))
        );
        assert_eq!(select(vec![address("127.0.0.2"), address("::1")]), None);
        assert!(!internal("169.254.1.1".parse().unwrap()));
        assert!(!internal("fc00::1".parse().unwrap()));
        assert_eq!(
            normalize(&Address {
                ip: "fe80::1".parse().unwrap(),
                scope: 7,
                scope_name: None,
            }),
            "[fe80:0:0:0:0:0:0:1%7]"
        );
    }

    #[test]
    fn linux_ipv6_scope_uses_interface_index_even_when_native_scope_is_zero() {
        assert_eq!(java_linux_ipv6_scope(0, 7), 7);
        let scoped = Address {
            ip: "2001:db8::1".parse().unwrap(),
            scope: java_linux_ipv6_scope(0, 7),
            scope_name: Some("eth0".to_owned()),
        };
        assert_eq!(normalize(&scoped), "[2001:db8:0:0:0:0:0:1%eth0]");
    }
}
