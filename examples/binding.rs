//! Binding configuration CLI for Matter devices
//!
//! Many Matter devices (for example a switch/remote) expose the **Binding**
//! cluster (0x001E). Its `Binding` attribute is a list of *targets* that tells
//! the device which other node(s)/endpoint(s) it should control directly. This
//! example lets you list, add, remove and clear those binding targets.
//!
//! # Usage
//!
//! ```bash
//! # Show the current binding table of endpoint 1
//! cargo run --example binding -- --device-address 192.168.1.100:5540 list
//!
//! # Bind endpoint 1 of this device to node 0x12 endpoint 1, On/Off cluster (0x0006)
//! cargo run --example binding -- --device-address 192.168.1.100:5540 \
//!     add-unicast --node 0x12 --target-endpoint 1 --cluster 0x0006
//!
//! # Bind to multicast group 5 for the On/Off cluster
//! cargo run --example binding -- --device-address 192.168.1.100:5540 \
//!     add-group --group 5 --cluster 0x0006
//!
//! # Remove all bindings that point at node 0x12
//! cargo run --example binding -- --device-address 192.168.1.100:5540 \
//!     remove --node 0x12
//!
//! # Clear the whole binding table
//! cargo run --example binding -- --device-address 192.168.1.100:5540 clear
//! ```

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use matc::clusters::codec::binding_cluster as binding;
use matc::clusters::codec::binding_cluster::Target;
use matc::{certmanager, controller, transport};

const DEFAULT_LOCAL_ADDRESS: &str = "0.0.0.0:5555";
const DEFAULT_CERT_PATH: &str = "./pem";

#[derive(Parser, Debug)]
#[command(about = "Configure Matter device bindings (Binding cluster 0x001E)")]
struct Cli {
    /// Enable verbose logging
    #[clap(long, default_value_t = false)]
    verbose: bool,

    /// Path to certificate directory
    #[clap(long, default_value_t = DEFAULT_CERT_PATH.to_string())]
    cert_path: String,

    /// Local UDP address to bind to
    #[clap(long, default_value_t = DEFAULT_LOCAL_ADDRESS.to_string())]
    local_address: String,

    /// Device address (IP:PORT) - required
    #[clap(long)]
    device_address: String,

    /// Controller ID (fabric identifier)
    #[clap(long, default_value_t = 100)]
    controller_id: u64,

    /// Device node ID
    #[clap(long, default_value_t = 300)]
    device_id: u64,

    /// Endpoint that hosts the Binding cluster (usually 1)
    #[clap(long, default_value_t = 1)]
    endpoint: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print the current binding table
    List,

    /// Add a unicast binding target (node + endpoint)
    AddUnicast {
        /// Target node ID
        #[clap(long, value_parser = parse_u64)]
        node: u64,
        /// Target endpoint on that node
        #[clap(long)]
        target_endpoint: u16,
        /// Optional cluster ID to restrict the binding to (e.g. 0x0006 for On/Off)
        #[clap(long, value_parser = parse_u32)]
        cluster: Option<u32>,
    },

    /// Add a groupcast binding target (multicast group)
    AddGroup {
        /// Target group ID
        #[clap(long)]
        group: u16,
        /// Optional cluster ID to restrict the binding to
        #[clap(long, value_parser = parse_u32)]
        cluster: Option<u32>,
    },

    /// Remove every binding matching the given (partial) filter
    Remove {
        #[clap(long, value_parser = parse_u64)]
        node: Option<u64>,
        #[clap(long)]
        group: Option<u16>,
        #[clap(long)]
        target_endpoint: Option<u16>,
        #[clap(long, value_parser = parse_u32)]
        cluster: Option<u32>,
    },

    /// Remove all bindings
    Clear,
}

/// Parse a u64 that may be given in decimal or `0x`-prefixed hex.
fn parse_u64(s: &str) -> Result<u64, String> {
    parse_radix(s).map(|v| v as u64)
}

/// Parse a u32 that may be given in decimal or `0x`-prefixed hex.
fn parse_u32(s: &str) -> Result<u32, String> {
    parse_radix(s).map(|v| v as u32)
}

fn parse_radix(s: &str) -> Result<u128, String> {
    let s = s.trim();
    let parsed = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u128::from_str_radix(hex, 16)
    } else {
        s.parse::<u128>()
    };
    parsed.map_err(|e| format!("invalid number '{}': {}", s, e))
}

/// Creates an authenticated CASE session to a Matter device
async fn create_connection(
    local_address: &str,
    device_address: &str,
    device_id: u64,
    controller_id: u64,
    cert_path: &str,
) -> Result<controller::Connection> {
    let cm: Arc<dyn certmanager::CertManager> = certmanager::FileCertManager::load(cert_path)
        .with_context(|| format!("Failed to load certificates from {}", cert_path))?;

    let transport = transport::Transport::new(local_address)
        .await
        .with_context(|| format!("Failed to create transport on {}", local_address))?;

    let controller = controller::Controller::new(&cm, &transport, cm.get_fabric_id())
        .context("Failed to create controller")?;

    let connection = transport.create_connection(device_address).await;

    controller
        .auth_sigma(&connection, device_id, controller_id)
        .await
        .with_context(|| {
            format!(
                "Failed to authenticate with device {} (node_id: {})",
                device_address, device_id
            )
        })
}

fn print_bindings(targets: &[Target]) {
    if targets.is_empty() {
        println!("Binding table is empty.");
        return;
    }
    println!("Binding table ({} entries):", targets.len());
    for (i, t) in targets.iter().enumerate() {
        let cluster = t
            .cluster
            .map(|c| format!("0x{:04X}", c))
            .unwrap_or_else(|| "all".to_string());
        match (t.node, t.endpoint, t.group) {
            (Some(node), Some(ep), _) => println!(
                "  [{}] unicast node=0x{:X} endpoint={} cluster={}",
                i, node, ep, cluster
            ),
            (_, _, Some(group)) => {
                println!("  [{}] group group={} cluster={}", i, group, cluster)
            }
            _ => println!("  [{}] {:?}", i, t),
        }
    }
}

/// Configures logging based on verbosity level
fn setup_logging(verbose: bool) {
    let log_level = if verbose {
        log::LevelFilter::Trace
    } else {
        log::LevelFilter::Error
    };

    env_logger::Builder::new()
        .parse_default_env()
        .target(env_logger::Target::Stdout)
        .filter_level(log_level)
        .format_line_number(true)
        .format_file(true)
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let connection = create_connection(
        &cli.local_address,
        &cli.device_address,
        cli.device_id,
        cli.controller_id,
        &cli.cert_path,
    )
    .await?;

    let ep = cli.endpoint;

    match cli.command {
        Commands::List => {
            let targets = binding::read_binding(&connection, ep)
                .await
                .context("Failed to read Binding attribute")?;
            print_bindings(&targets);
        }
        Commands::AddUnicast { node, target_endpoint, cluster } => {
            binding::add_binding(&connection, ep, Target::unicast(node, target_endpoint, cluster))
                .await
                .context("Failed to add unicast binding")?;
            println!("Unicast binding added.");
            print_bindings(&binding::read_binding(&connection, ep).await?);
        }
        Commands::AddGroup { group, cluster } => {
            binding::add_binding(&connection, ep, Target::group(group, cluster))
                .await
                .context("Failed to add group binding")?;
            println!("Group binding added.");
            print_bindings(&binding::read_binding(&connection, ep).await?);
        }
        Commands::Remove { node, group, target_endpoint, cluster } => {
            let filter = Target { node, group, endpoint: target_endpoint, cluster };
            let removed = binding::remove_binding(&connection, ep, &filter)
                .await
                .context("Failed to remove binding(s)")?;
            println!("Removed {} binding(s).", removed);
            print_bindings(&binding::read_binding(&connection, ep).await?);
        }
        Commands::Clear => {
            binding::clear_binding(&connection, ep)
                .await
                .context("Failed to clear bindings")?;
            println!("Binding table cleared.");
        }
    }

    Ok(())
}
