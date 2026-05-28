//! Example: simple L4 proxy

use std::{env, io, process};

use tokio::net::{TcpListener, TcpStream};

#[tokio::main(flavor = "current_thread")]
// #[tokio::main]
async fn main() -> io::Result<()> {
    println!("PID is {}", std::process::id());

    let (listen_addr, upstream_addr) = parse_args();

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

    let worker_threads = tokio::runtime::Handle::current().metrics().num_workers();
    let pipe_size = splicer::pipe::Pipe::new()?.size();
    println!(
        "tokio worker threads: {worker_threads}, splice pipe size: {pipe_size} bytes (per direction, 2 per connection)"
    );

    tokio::select! {
        res = serve(listen_addr, upstream_addr) => {
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

fn parse_args() -> (String, String) {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: {} <listen_port> <upstream_port>", args[0]);
        process::exit(2);
    }
    let listen_port: u16 = args[1].parse().expect("invalid listen port");
    let upstream_port: u16 = args[2].parse().expect("invalid upstream port");
    (
        format!("0.0.0.0:{listen_port}"),
        format!("127.0.0.1:{upstream_port}"),
    )
}

async fn serve(listen_addr: String, upstream_addr: String) -> io::Result<()> {
    let listener = TcpListener::bind(&listen_addr).await?;

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

        tokio::spawn(forwarding(incoming, upstream_addr.clone()));
    }
}

async fn forwarding(mut stream1: TcpStream, upstream_addr: String) -> io::Result<()> {
    let stream2 = TcpStream::connect(&upstream_addr).await;

    let mut stream2 = match stream2 {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to remote server: {e}");
            return Err(e);
        }
    };

    let instant = std::time::Instant::now();

    let io_sl2sr = splicer::splice::SpliceCtx::new()?.into();
    let io_sr2sl = splicer::splice::SpliceCtx::new()?.into();

    let traffic = splicer::io::SpliceBidiIo { io_sl2sr, io_sr2sl }
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
