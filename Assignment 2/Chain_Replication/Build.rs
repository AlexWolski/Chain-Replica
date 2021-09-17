// Build file from the article: A beginner's guide to gRPC with Rust
// https://dev.to/anshulgoyal15/a-beginners-guide-to-grpc-with-rust-3c7o
// https://github.com/anshulrgoyal/rust-grpc-demo

 fn main()->Result<(),Box<dyn std::error::Error>> {
   // Compiling protos using path on build time
    tonic_build::compile_protos("proto/chain.proto")?;
    Ok(())
 }