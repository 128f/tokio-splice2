//! Example: simple L4 proxy

use std::{env, io};

use tokio::net::{TcpListener, TcpStream};

// #[tokio::main(flavor = "current_thread")]
#[tokio::main]
async fn main() -> io::Result<()> {
    println!("PID is {}", std::process::id());

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, Layer};

    let (w, _g) = tracing_appender::non_blocking(io::stdout());
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(w).with_filter(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::DEBUG.into())
            .from_env_lossy()
            .add_directive("otel::tracing=trace".parse().unwrap())
            .add_directive("h2=error".parse().unwrap())
            .add_directive("tower=error".parse().unwrap())
            .add_directive("hyper=error".parse().unwrap()),
    );

    tracing_subscriber::registry()
        .with(fmt_layer)
        // .with(console_subscriber::spawn())
        .init();

    tokio::select! {
        res = serve() => {
            if let Err(err) = res {
                eprintln!("Serve failed {err}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl + C, shutting down");
        }
    }

    Ok(())
}

async fn serve() -> io::Result<()> {
    let listener = TcpListener::bind(
        env::var("EXAMPLE_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:5201".to_string()),
    )
    .await?;

    loop {
        let (incoming, remote_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {
                println!("Connection aborted.");
                continue;
            }
            Err(e) => {
                eprintln!("Failed to accept: {e:#?}");
                break Err(e);
            }
        };

        println!("Process incoming connection from {remote_addr}");

        tokio::spawn(forwarding(incoming));
    }
}

async fn forwarding(mut stream1: TcpStream) -> io::Result<()> {
    let stream2 = TcpStream::connect(
        env::var("EXAMPLE_REMOTE_ADDR").unwrap_or_else(|_| "127.0.0.1:5202".to_string()),
    )
    .await;

    let mut stream2 = match stream2 {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to remote server: {e}");
            return Err(e);
        }
    };

    let instant = std::time::Instant::now();

    let io_sl2sr = tokio_splice2::splice::Splicer::new()?.into();
    let io_sr2sl = tokio_splice2::splice::Splicer::new()?.into();

    let traffic = tokio_splice2::io::SpliceBidiIo { io_sl2sr, io_sr2sl }
        .execute(&mut stream1, &mut stream2)
        .await;

    let total = traffic.sum();
    let cost = instant.elapsed();
    println!(
        "Forwarded traffic: total: {total} B, time: {:.2} s, avg: {:.4} B/s, error: {:?}",
        cost.as_secs_f64(),
        total as f64 / cost.as_secs_f64(),
        traffic.error
    );

    Ok(())
}
