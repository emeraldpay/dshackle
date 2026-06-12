use shadow_rs::ShadowBuilder;

fn main() {
    ShadowBuilder::builder()
        .deny_const(Default::default())
        .build()
        .unwrap();

    // Messages for the values cached in Redis; requires `protoc` on the PATH
    prost_build::compile_protos(&["proto/cache.proto"], &["proto/"])
        .expect("failed to compile proto/cache.proto (is protoc installed?)");
}
