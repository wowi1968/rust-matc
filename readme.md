# simple rust matter protocol library

![build](https://github.com/tom-code/rust-matc/actions/workflows/rust.yml/badge.svg)
[![doc](https://github.com/tom-code/rust-matc/actions/workflows/doc.yml/badge.svg)](https://tom-code.github.io/rust-matc/matc/)

This is matter protocol library in rust (controller side).

[API doc](https://tom-code.github.io/rust-matc/matc/)

[Examples](https://github.com/tom-code/rust-matc/tree/main/examples)

[Matter controller desktop application also demonstrates use of this library](https://github.com/tom-code/matc-ui)

It supports controller side of:

* PASE - passcode authenticated session establishment
  * variant of [spake 2+](https://datatracker.ietf.org/doc/rfc9383/)
* CASE - certificate authenticated session establishment
  * variant of [SIGMA](https://scispace.com/pdf/sigma-the-sign-and-mac-approach-to-authenticated-diffie-103fql3b25.pdf)
* Commissioning procedure
  * sign and push certificates to device
  * BLE commissioning with Wi-Fi/Thread credential provisioning (opt-in, `--features ble`)
* Basic interactions
  * Read/Write attributes
  * Invoke commands
  * Subscribe for notifications

See [examples](https://github.com/tom-code/rust-matc/tree/main/examples) for usage, including the high-level `devman_demo`, BLE commissioning, and the low-level `demo` CLI.

## BLE proxy

With `--features ble`, BLE uses the local Bluetooth adapter by default. An
application can instead implement the transport-neutral
[`BleProxy`](src/ble/proxy.rs) and `BleGattConnection` traits to bridge to a
Bluetooth proxy, for example in an iOS or Android app. No network protocol or
mobile implementation is imposed or included.

```rust
use std::sync::Arc;
use matc::ble::{self, BleProxy};

fn configure_mobile_proxy(proxy: Arc<dyn BleProxy>) {
    ble::set_proxy(Some(proxy));
}

fn restore_local_bluetooth() {
    ble::set_proxy(None);
}
```

Configure the proxy before scanning or commissioning. The setting is process-wide
and applies to `ble::scan_commissionable`, `ble::find_by_discriminator`,
`Controller::commission_ble`, and `DeviceManager::commission_ble_with_code`.
Those existing calls need no other changes. When configured, the proxy is used
without initializing the local BLE adapter; proxy errors do not trigger a local
fallback. Resetting the setting affects only new operations. In-flight operations
and discovered peripheral handles keep their original backend.

`ble::proxy_active()` reports whether a proxy is configured. At `info` log level
(target `matc::ble`), changes to the proxy setting and every scan, search, and connect
are logged with the backend used ("via proxy" or "via local Bluetooth"), for example
with `RUST_LOG=matc::ble=info`.

The application adapter must:

* Scan for the requested duration and return advertisements with the raw Matter
  service data for UUID `0000fff6-0000-1000-8000-00805f9b34fb`. Peripheral addresses
  are opaque identifiers understood by that proxy, not necessarily MAC addresses.
* Connect to the requested peripheral, discover Matter C1/C2, and report the
  negotiated ATT MTU (including ATT overhead; use 23 if a larger value is unknown).
* Perform C1 writes **with response**, reporting completion only when the remote
  write succeeds. rust-matc enables C2 indications after the first handshake write.
* Buffer C2 indications during subscription, forward them in order as raw bytes,
  and support concurrent reads and writes. Return errors on transport failures
  and `None` on a normal disconnect. Operations must be cancellation-safe.
* Release the connection and subscriptions on `disconnect`. Cleanup is requested
  asynchronously when the peripheral/BTP connection is dropped, including failed
  or cancelled BTP handshakes; keep the Tokio runtime alive to complete it.

BTP, PASE, commissioning, and Wi-Fi/Thread credential provisioning stay in
rust-matc. Protect and authenticate any remote transport: commissioning data must
not be exposed over an untrusted connection.

`CommissionableDevice::peripheral` is now a backend-neutral `PeripheralHandle`.
Pass it to `ble::connect_peripheral` as before; that function also still accepts
a native btleplug `Peripheral` for explicitly local connections. Code that accessed
btleplug-specific methods through the field must match `PeripheralHandle::Local`.
