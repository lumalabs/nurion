fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false) // We use Python grpcio for client
        .compile_protos(&["proto/workqueue.proto"], &["proto/"])?;
    Ok(())
}
