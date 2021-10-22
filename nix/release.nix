# Use this file with nix-build-uncached on CI
{ system ? builtins.currentSystem }:
let
  nixpkgs = import ./nixpkgs.nix { inherit system; };

  # This is used to wrap all of our outputs, so they all end-up in the cache.
  #
  # There are two attributes ` allowSubstitutes = false;` and
  # `preferLocalBuild = true;` that influence how derivations gets pulled and
  # pushed.
  forceCached = import ./lib/force_cached.nix nixpkgs.coreutils;

  # All the packages that we care about
  ourPackages = import ./. { inherit nixpkgs system; };
in
forceCached ourPackages
