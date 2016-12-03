{ pkgs ? import <nixpkgs> {} }:

pkgs.stdenv.mkDerivation {
  name = "rethinkdb-dev";
  buildInputs = with pkgs; [
   gcc6
   protobuf
   python
   nodejs
   nodePackages.coffee-script
   nodePackages.browserify
   zlib
   openssl
   curl
   jemalloc
   boost
   git
  ];
  shellHook = ''
    export LIBRARY_PATH=${pkgs.jemalloc}/lib
  '';
}
