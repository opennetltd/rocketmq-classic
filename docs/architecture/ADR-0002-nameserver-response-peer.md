# ADR-0002: Return the selected NameServer with its reply

Phase 5 requires response errors to identify the peer. Phase 3's `invoke_name_server` returns only the command, and reading the shared chosen address after an invocation races other calls or address-list updates.

Add `invoke_name_server_with_peer` returning the response and the address used by that invocation. Retain the existing `invoke_name_server` signature by delegating and discarding the peer; transport selection, failover and deadline remain unchanged. The facade uses the returned address, not global selector state. Loopback tests cover failover and the response peer.