{
  lib,
  config,
  inputs,
  ...
}:
{
  perSystem =
    { pkgs, ... }:
    let
      inherit (config.flake.lib) mkShell mkDirtyBuild;

      # The absolute work tree path, as a string. Reuses the `devenv-root`
      # workaround input which already carries it (see `lib/shell.parts.nix`).
      rootDir = lib.strings.trim (builtins.readFile inputs.devenv-root.outPath);

      shell = mkShell {
        inherit inputs pkgs;
        modules = [
          {
            quitsh.toolchains = [ "build-go" ];
            quitsh.languages.go = {
              enable = true;
              package = pkgs.go_1_26;
            };
          }
        ];
      };
    in
    {
      # Build the quitsh CLI straight out of the dirty work tree.
      # `inputHash` would be supplied by quitsh from the target's `inputs`
      # patterns; hard-coded here so the caching behaviour is observable.
      packages.dirty-cli =
        mkDirtyBuild
          {
            inherit pkgs shell rootDir;
            name = "dirty-cli";
            inputHash = "demo-0";
          }
          ''
            mkdir -p "$out/bin"
            cd tools/cli
            go build -o "$out/bin/quitsh-cli" ./cmd/cli
          '';
    };
}
