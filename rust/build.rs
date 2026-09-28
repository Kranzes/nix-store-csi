use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=CSI_PROTO");
    let proto = std::env::var_os("CSI_PROTO")
        .ok_or("CSI_PROTO isn't set to the CSI spec's csi.proto, `mkShell` sets it")?;
    let proto = Path::new(&proto);
    // Kubelet is the client, so only the server side is generated.
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&[proto], &[proto.parent().unwrap_or(Path::new("."))])?;
    Ok(())
}
