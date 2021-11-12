{ stdenv, runCommand, fetchurl, autoPatchelfHook, lib }:

stdenv.mkDerivation rec {
  pname = "protoc-gen-grpc-java";
  version = "1.38.0";

  src = fetchurl {
    url = "https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/${version}/protoc-gen-grpc-java-${version}-linux-x86_64.exe";
    sha256 = "sha256:1gy5lkxj6d4vrgalwnjp15biybcnrmp695si9878y7high39ymr3";
  };

  nativeBuildInputs = [ autoPatchelfHook ];

  unpackPhase = ''
    cp $src ./bin
    chmod +x ./bin
  '';

  installPhase = ''
    mkdir -p $out/bin
    mv ./bin $out/bin/$pname
  '';
}
