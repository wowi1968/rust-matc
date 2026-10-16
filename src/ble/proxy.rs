use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{parse_service_data, CommissionableDevice, PeripheralHandle};
use crate::btp::{BleCleanup, BlePeripheral};

/// Advertisement reported by a proxy. Identifiers are opaque and scoped to it.
#[derive(Debug, Clone)]
pub struct BleAdvertisement {
    pub address: String,
    pub name: Option<String>,
    pub rssi: Option<i16>,
    pub tx_power: Option<i16>,
    /// Raw service data for Matter service UUID 0xFFF6, not the full advertisement.
    pub service_data: Vec<u8>,
}

/// Implement in the host application to bridge to a mobile or remote BLE stack.
#[async_trait]
pub trait BleProxy: Send + Sync {
    /// Scan for at most `timeout`, returning the advertisements collected.
    /// An empty result means no devices; transport/permission errors must be errors.
    async fn scan(&self, timeout: Duration) -> Result<Vec<BleAdvertisement>>;

    /// Connect and discover the Matter service and its C1/C2 characteristics.
    /// Each call must return an independent connection, ready for C1 writes.
    async fn connect(&self, address: &str) -> Result<Arc<dyn BleGattConnection>>;
}

/// One connected Matter peripheral. BTP and Matter remain in rust-matc.
///
/// Implementations must support concurrent writes and notification reads.
/// Async operations must be cancellation-safe; disconnect must also release any
/// pending reads. Buffer indications from the moment subscription starts.
#[async_trait]
pub trait BleGattConnection: Send + Sync {
    /// Actual negotiated ATT MTU, including the three-byte ATT overhead.
    /// Use 23 if the platform cannot report a larger negotiated value.
    fn att_mtu(&self) -> u16;

    /// Write C1 (18ee2ef5-263d-4559-959f-4f9c429f9d11) with response.
    /// Complete only after the remote GATT write has succeeded.
    async fn write_c1(&self, data: &[u8]) -> Result<()>;

    /// Enable C2 (18ee2ef5-263d-4559-959f-4f9c429f9d12) indications.
    /// rust-matc calls this after the first C1 handshake write.
    async fn subscribe_c2(&self) -> Result<()>;

    /// Read the next C2 indication, in order and without modification.
    /// Return `None` only when disconnected, and `Err` on transport failure.
    async fn next_c2(&self) -> Result<Option<Vec<u8>>>;

    /// Release the remote peripheral and subscription.
    async fn disconnect(&self) -> Result<()>;
}

pub(super) async fn scan(
    proxy: Arc<dyn BleProxy>,
    timeout: Duration,
) -> Result<Vec<CommissionableDevice>> {
    let advertisements = proxy.scan(timeout).await.context("BLE proxy scan")?;
    Ok(advertisements
        .into_iter()
        .filter_map(|advertisement| {
            if advertisement.service_data.len() < 8 {
                return None;
            }
            let (discriminator, vendor_id, product_id, cm_flag) =
                parse_service_data(&advertisement.service_data);
            Some(CommissionableDevice {
                discriminator,
                vendor_id,
                product_id,
                cm_flag,
                rssi: advertisement.rssi,
                name: advertisement.name,
                tx_power: advertisement.tx_power,
                peripheral: PeripheralHandle::Proxy {
                    proxy: proxy.clone(),
                    address: advertisement.address.clone(),
                },
                address: advertisement.address,
            })
        })
        .collect())
}

pub(super) async fn find(
    proxy: Arc<dyn BleProxy>,
    discriminator: u16,
    short_match: bool,
    timeout: Duration,
) -> Result<BlePeripheral> {
    for device in scan(proxy, timeout).await? {
        let matches = if short_match {
            device.discriminator >> 8 == discriminator >> 8
        } else {
            device.discriminator == discriminator
        };
        if matches {
            return super::connect_peripheral(device.peripheral).await;
        }
    }
    bail!(
        "BLE scan timeout: no device with discriminator {} found",
        discriminator
    )
}

pub(super) async fn connect(proxy: Arc<dyn BleProxy>, address: &str) -> Result<BlePeripheral> {
    let connection = proxy.connect(address).await.context("BLE proxy connect")?;
    let att_mtu = connection.att_mtu();
    if !(23..=517).contains(&att_mtu) {
        if let Err(error) = connection.disconnect().await {
            log::warn!("BLE proxy disconnect after invalid MTU: {error:#}");
        }
        bail!("BLE proxy returned invalid ATT MTU: {att_mtu}");
    }
    log::info!("BLE: connected to {address} via proxy (ATT MTU {att_mtu})");

    let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(64);
    let (read_tx, read_rx) = mpsc::channel::<Vec<u8>>(64);
    // Do not read until subscription has completed, but let the adapter buffer
    // indications that arrive during subscribe_c2.
    let (subscribed_tx, subscribed_rx) = tokio::sync::oneshot::channel();
    let reader = connection.clone();
    let read_task = tokio::spawn(async move {
        if subscribed_rx.await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                _ = read_tx.closed() => break,
                result = reader.next_c2() => match result {
                    Ok(Some(data)) => {
                        if read_tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        log::warn!("BLE proxy C2 read error: {error:#}");
                        break;
                    }
                }
            }
        }
    });
    let c2_abort = read_task.abort_handle();
    let reader_abort = c2_abort.clone();
    let writer = connection.clone();
    let write_task = tokio::spawn(async move {
        let mut subscribed_tx = Some(subscribed_tx);
        while let Some(data) = write_rx.recv().await {
            if let Err(error) = writer.write_c1(&data).await {
                log::warn!("BLE proxy C1 write error: {error:#}");
                reader_abort.abort();
                break;
            }
            if let Some(ready) = subscribed_tx.take() {
                if let Err(error) = writer.subscribe_c2().await {
                    log::warn!("BLE proxy C2 subscribe error: {error:#}");
                    reader_abort.abort();
                    break;
                }
                let _ = ready.send(());
            }
        }
    });
    let writer_abort = write_task.abort_handle();
    let disconnect_reader = c2_abort.clone();
    let disconnect = Box::new(move || {
        writer_abort.abort();
        disconnect_reader.abort();
        tokio::spawn(async move {
            if let Err(error) = connection.disconnect().await {
                log::warn!("BLE proxy disconnect error: {error:#}");
            }
        });
    });
    Ok(BlePeripheral {
        write_c1: write_tx,
        read_c2: read_rx,
        att_mtu: usize::from(att_mtu),
        cleanup: BleCleanup::new(c2_abort, disconnect),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{btp::BtpConnection, transport::ConnectionTrait};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    struct MockConnection {
        mtu: u16,
        events: Mutex<Vec<String>>,
        writes: Mutex<Vec<Vec<u8>>>,
        incoming: tokio::sync::Mutex<mpsc::Receiver<Result<Option<Vec<u8>>>>>,
        disconnected: AtomicUsize,
        fail_write: bool,
        fail_subscribe: bool,
    }

    #[async_trait]
    impl BleGattConnection for MockConnection {
        fn att_mtu(&self) -> u16 {
            self.mtu
        }

        async fn write_c1(&self, data: &[u8]) -> Result<()> {
            self.events.lock().unwrap().push("write".into());
            if self.fail_write {
                bail!("write failed");
            }
            self.writes.lock().unwrap().push(data.to_vec());
            Ok(())
        }

        async fn subscribe_c2(&self) -> Result<()> {
            self.events.lock().unwrap().push("subscribe".into());
            if self.fail_subscribe {
                bail!("subscribe failed");
            }
            Ok(())
        }

        async fn next_c2(&self) -> Result<Option<Vec<u8>>> {
            self.incoming
                .lock()
                .await
                .recv()
                .await
                .context("mock stream ended")?
        }

        async fn disconnect(&self) -> Result<()> {
            self.disconnected.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockProxy {
        advertisements: Vec<BleAdvertisement>,
        connection: Arc<MockConnection>,
        addresses: Mutex<Vec<String>>,
        timeouts: Mutex<Vec<Duration>>,
        fail_scan: bool,
        fail_connect: bool,
    }

    #[async_trait]
    impl BleProxy for MockProxy {
        async fn scan(&self, timeout: Duration) -> Result<Vec<BleAdvertisement>> {
            self.timeouts.lock().unwrap().push(timeout);
            if self.fail_scan {
                bail!("proxy offline");
            }
            Ok(self.advertisements.clone())
        }

        async fn connect(&self, address: &str) -> Result<Arc<dyn BleGattConnection>> {
            self.addresses.lock().unwrap().push(address.into());
            if self.fail_connect {
                bail!("connect failed");
            }
            Ok(self.connection.clone())
        }
    }

    fn fixture() -> (MockProxy, mpsc::Sender<Result<Option<Vec<u8>>>>) {
        let (tx, rx) = mpsc::channel(16);
        (
            MockProxy {
                advertisements: vec![BleAdvertisement {
                    address: "ios-peripheral-id".into(),
                    name: Some("Matter device".into()),
                    rssi: Some(-42),
                    tx_power: Some(4),
                    service_data: vec![0, 0xbc, 0x1a, 0x34, 0x12, 0x78, 0x56, 0],
                }],
                connection: Arc::new(MockConnection {
                    mtu: 185,
                    events: Mutex::new(Vec::new()),
                    writes: Mutex::new(Vec::new()),
                    incoming: tokio::sync::Mutex::new(rx),
                    disconnected: AtomicUsize::new(0),
                    fail_write: false,
                    fail_subscribe: false,
                }),
                addresses: Mutex::new(Vec::new()),
                timeouts: Mutex::new(Vec::new()),
                fail_scan: false,
                fail_connect: false,
            },
            tx,
        )
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mock operation timed out");
    }

    #[tokio::test]
    async fn scan_filters_and_parses_matter_service_data() {
        let (mut proxy, _tx) = fixture();
        let mut invalid = proxy.advertisements[0].clone();
        invalid.service_data.truncate(7);
        proxy.advertisements.push(invalid);
        let proxy = Arc::new(proxy);
        let devices = scan(proxy.clone(), Duration::from_secs(3)).await.unwrap();
        assert_eq!(devices.len(), 1);
        let device = &devices[0];
        assert_eq!(device.discriminator, 0xabc);
        assert_eq!(device.vendor_id, 0x1234);
        assert_eq!(device.product_id, 0x5678);
        assert!(device.cm_flag);
        assert_eq!(device.rssi, Some(-42));
        assert_eq!(device.tx_power, Some(4));
        assert_eq!(device.name.as_deref(), Some("Matter device"));
        assert_eq!(device.address, "ios-peripheral-id");
        assert_eq!(*proxy.timeouts.lock().unwrap(), [Duration::from_secs(3)]);
    }

    #[tokio::test]
    async fn find_matches_full_and_short_discriminators() {
        for (discriminator, short) in [(0xabc, false), (0xa00, true)] {
            let (proxy, _tx) = fixture();
            let proxy = Arc::new(proxy);
            let peripheral = find(proxy.clone(), discriminator, short, Duration::from_secs(2))
                .await
                .unwrap();
            assert_eq!(*proxy.addresses.lock().unwrap(), ["ios-peripheral-id"]);
            drop(peripheral);
            wait_for(|| proxy.connection.disconnected.load(Ordering::SeqCst) == 1).await;
        }
        for (discriminator, short) in [(0xabd, false), (0xb00, true)] {
            let (proxy, _tx) = fixture();
            let proxy = Arc::new(proxy);
            assert!(find(proxy.clone(), discriminator, short, Duration::ZERO)
                .await
                .is_err());
            assert!(proxy.addresses.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn configured_proxy_routes_public_apis_and_handles_keep_their_backend() {
        struct ResetProxy;
        impl Drop for ResetProxy {
            fn drop(&mut self) {
                crate::ble::set_proxy(None);
            }
        }
        let _reset = ResetProxy;
        assert!(crate::ble::current_proxy().is_none());
        let (proxy, _tx) = fixture();
        let proxy = Arc::new(proxy);
        crate::ble::set_proxy(Some(proxy.clone()));
        let mut devices = crate::ble::scan_commissionable(Duration::ZERO)
            .await
            .unwrap();
        let peripheral = crate::ble::find_by_discriminator(0xabc, false, Duration::ZERO)
            .await
            .unwrap();
        drop(peripheral);
        wait_for(|| proxy.connection.disconnected.load(Ordering::SeqCst) == 1).await;
        crate::ble::set_proxy(None);
        assert!(crate::ble::current_proxy().is_none());
        let peripheral = crate::ble::connect_peripheral(devices.remove(0).peripheral)
            .await
            .unwrap();
        assert_eq!(proxy.addresses.lock().unwrap().len(), 2);
        drop(peripheral);
        wait_for(|| proxy.connection.disconnected.load(Ordering::SeqCst) == 2).await;

        let (mut offline, _tx) = fixture();
        offline.fail_scan = true;
        crate::ble::set_proxy(Some(Arc::new(offline)));
        let error = crate::ble::scan_commissionable(Duration::ZERO)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("proxy offline"));
    }

    #[tokio::test]
    async fn forwards_bytes_and_subscribes_once_after_first_write() {
        let (proxy, tx) = fixture();
        let connection = proxy.connection.clone();
        let mut peripheral = connect(Arc::new(proxy), "device").await.unwrap();
        assert_eq!(peripheral.att_mtu, 185);
        assert!(connection.events.lock().unwrap().is_empty());
        peripheral.write_c1.send(vec![1, 2, 3]).await.unwrap();
        peripheral.write_c1.send(vec![4, 5]).await.unwrap();
        wait_for(|| connection.writes.lock().unwrap().len() == 2).await;
        assert_eq!(
            *connection.events.lock().unwrap(),
            ["write", "subscribe", "write"]
        );
        assert_eq!(
            *connection.writes.lock().unwrap(),
            [vec![1, 2, 3], vec![4, 5]]
        );
        tx.send(Ok(Some(vec![6, 7, 8]))).await.unwrap();
        assert_eq!(peripheral.read_c2.recv().await.unwrap(), [6, 7, 8]);
        drop(peripheral);
        wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
    }

    #[tokio::test]
    async fn btp_handshake_and_bidirectional_messages_use_proxy() {
        let (proxy, tx) = fixture();
        let connection = proxy.connection.clone();
        tx.send(Ok(Some(vec![0x65, 0x6c, 4, 185, 0, 6])))
            .await
            .unwrap();
        let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
        let btp = BtpConnection::connect(peripheral).await.unwrap();
        assert_eq!(
            connection.writes.lock().unwrap()[0],
            [0x65, 0x6c, 4, 0, 0, 0, 185, 0, 6]
        );
        btp.send(&[10, 11]).await.unwrap();
        wait_for(|| connection.writes.lock().unwrap().len() >= 2).await;
        assert!(connection.writes.lock().unwrap()[1].ends_with(&[10, 11]));
        tx.send(Ok(Some(vec![0x05, 0, 2, 0, 20, 21])))
            .await
            .unwrap();
        assert_eq!(btp.receive(Duration::from_secs(1)).await.unwrap(), [20, 21]);
        drop(btp);
        wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
    }

    #[tokio::test]
    async fn write_and_subscribe_failures_close_btp_and_disconnect() {
        for fail_write in [true, false] {
            let (mut proxy, _tx) = fixture();
            let connection = Arc::get_mut(&mut proxy.connection).unwrap();
            connection.fail_write = fail_write;
            connection.fail_subscribe = !fail_write;
            let connection = proxy.connection.clone();
            let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
            let error = BtpConnection::connect(peripheral).await.err().unwrap();
            assert!(format!("{error:#}").contains("C2 stream closed"));
            wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
        }
    }

    #[tokio::test]
    async fn read_errors_and_remote_disconnect_close_channel() {
        for result in [Err(anyhow::anyhow!("proxy transport lost")), Ok(None)] {
            let (proxy, tx) = fixture();
            let connection = proxy.connection.clone();
            let mut peripheral = connect(Arc::new(proxy), "device").await.unwrap();
            peripheral.write_c1.send(vec![1]).await.unwrap();
            tx.send(result).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(1), peripheral.read_c2.recv())
                    .await
                    .unwrap()
                    .is_none()
            );
            drop(peripheral);
            wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
        }
    }

    #[tokio::test]
    async fn failed_and_cancelled_handshakes_disconnect() {
        let (proxy, tx) = fixture();
        let connection = proxy.connection.clone();
        tx.send(Ok(Some(vec![0]))).await.unwrap();
        let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
        assert!(BtpConnection::connect(peripheral).await.is_err());
        wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;

        let (proxy, _tx) = fixture();
        let connection = proxy.connection.clone();
        let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
        let handshake = tokio::spawn(BtpConnection::connect(peripheral));
        wait_for(|| !connection.writes.lock().unwrap().is_empty()).await;
        handshake.abort();
        assert!(handshake.await.err().unwrap().is_cancelled());
        wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
    }

    #[tokio::test]
    async fn invalid_mtu_and_connection_errors_are_reported() {
        for mtu in [23, 517] {
            let (mut proxy, _tx) = fixture();
            Arc::get_mut(&mut proxy.connection).unwrap().mtu = mtu;
            let connection = proxy.connection.clone();
            let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
            assert_eq!(peripheral.att_mtu, usize::from(mtu));
            drop(peripheral);
            wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
        }
        for mtu in [0, 22, 518] {
            let (mut proxy, _tx) = fixture();
            Arc::get_mut(&mut proxy.connection).unwrap().mtu = mtu;
            let connection = proxy.connection.clone();
            assert!(connect(Arc::new(proxy), "device").await.is_err());
            assert_eq!(connection.disconnected.load(Ordering::SeqCst), 1);
        }
        let (mut proxy, _tx) = fixture();
        proxy.fail_connect = true;
        let error = connect(Arc::new(proxy), "device").await.err().unwrap();
        assert!(format!("{error:#}").contains("connect failed"));
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_timeout_disconnects() {
        let (proxy, _tx) = fixture();
        let connection = proxy.connection.clone();
        let peripheral = connect(Arc::new(proxy), "device").await.unwrap();
        let error = BtpConnection::connect(peripheral).await.err().unwrap();
        assert!(format!("{error:#}").contains("handshake timeout"));
        wait_for(|| connection.disconnected.load(Ordering::SeqCst) == 1).await;
    }
}
