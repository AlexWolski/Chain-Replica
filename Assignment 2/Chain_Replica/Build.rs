// Build file from the article: Rust Meets gRPC - Using Tonic
// https://www.swiftdiaries.com/rust/tonic/

fn main() {
    let proto_files = &["proto/chain.proto"];
    let proto_dependencies = &["proto"];

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(proto_files, proto_dependencies)
        .expect("Error generating protobuf");
}