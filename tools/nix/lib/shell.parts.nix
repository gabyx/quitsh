{
  lib,
  config,
  ...
}:
{
  flake.lib = {
    # Dump the environment of a dev shell `shell` into a *sourceable* bash
    # script derivation.
    #
    # A `mkShell` derivation is a normal derivation: by the time its
    # `buildPhase` runs, `stdenv`'s setup has already assembled `PATH`,
    # `shellHook` and every env. variable contributed by the shell's inputs.
    # We simply serialize that environment with `declare -px`.
    #
    # Build-sandbox-local variables are stripped, otherwise sourcing the result
    # would clobber `$out`, `$TMPDIR`, ... of the *consuming* derivation.
    #
    # NOTE: `nix print-dev-env` does the same thing, but it is a CLI command
    #       and therefore not usable from within Nix.
    mkShellEnv =
      shell:
      shell.overrideAttrs (old: {
        name = "${old.name or "shell"}-env";

        phases = [ "buildPhase" ];
        buildPhase = ''
          # NOTE: The subshell keeps the `unset`s away from the `$out`
          #       redirection, which the parent shell expands beforehand.
          (
            for v in \
              out outputs name system builder args stdenv \
              phases buildPhase \
              NIX_BUILD_TOP NIX_BUILD_CORES NIX_LOG_FD NIX_ENFORCE_PURITY \
              TMP TMPDIR TEMP TEMPDIR HOME PWD OLDPWD SHLVL TERM; do
              unset "$v" 2>/dev/null || true
            done

            declare -px
          ) > "$out"
        '';
      });

    # Run `text` inside the environment of the dev shell `shell`, but in a
    # plain (sandboxed) derivation instead of over `nix develop`.
    #
    # - `enterShell`: also run the shell's `shellHook` (devenv's `enterShell`).
    #   Off by default: devenv's hook is impure, it writes to `devenv.root`.
    runInShell =
      {
        pkgs,
        name,
        shell,
        enterShell ? false,
        derivationArgs ? { },
      }:
      text:
      pkgs.runCommandWith
        {
          inherit name;

          derivationArgs = {
            # The toolchain closure comes along as a reference of the script.
            shellEnv = config.flake.lib.mkShellEnv shell;

            # Normally exported by devenv's `enterShell` which we do not run.
            # Without it `quitsh` does not detect the toolchain and dispatches
            # into it again, endlessly.
            QUITSH_TOOLCHAINS = lib.concatStringsSep "," (shell.config.quitsh.toolchains or [ ]);
          }
          // derivationArgs;
        }
        ''
          source "$shellEnv"
          ${lib.optionalString enterShell ''eval "$shellHook"''}

          ${text}
        '';

    # Build inline commands into a derivation *dirtily*: the builder runs
    # unsandboxed, `cd`-ed into the live work tree, with the toolchain of
    # `shell` on `PATH`. Uncommitted changes, the network and warm build
    # caches (e.g. devenv's `GOPATH`) are all visible, but the result is still
    # a normal store path other derivations can consume.
    #
    # This is what `flox build` does without `sandbox = "pure"`.
    #
    # IMPORTANT: Nix caches this on `inputHash`, *not* on the work tree. Pass a
    # fingerprint of the target's declared inputs - quitsh knows these from the
    # `inputs` patterns in `.component.yaml` - otherwise the build will not
    # re-run when you edit a file.
    #
    # NOTE: Requires `--option sandbox relaxed` (or `sandbox = relaxed` in
    #       `nix.conf`). With `sandbox = true` the `__noChroot` is silently
    #       ignored and the build fails on the missing work tree.
    #
    # NOTE: `rootDir` must be a *string*, not a Nix path, otherwise the work
    #       tree is copied into the store and the whole point is lost.
    mkDirtyBuild =
      {
        pkgs,
        name,
        shell,
        rootDir,
        # Writable HOME for the build user; defaults to a per-build temp dir.
        # Point it at a shared writable path to keep caches warm across builds.
        homeDir ? null,
        inputHash,
        enterShell ? false,
        derivationArgs ? { },
      }:
      text:
      let
        homeDirExpr = if homeDir == null then "$TMPDIR/home" else homeDir;
      in
      assert lib.assertMsg (
        !lib.isPath rootDir
      ) "`rootDir` must be a string, a path would be copied into the store.";
      pkgs.runCommandWith
        {
          inherit name;

          derivationArgs = {
            # Escape the sandbox: see the note above.
            __noChroot = true;

            # The cache key. Changing it forces a rebuild, keeping it forces a
            # cache hit - regardless of what the work tree actually looks like.
            inherit inputHash;

            shellEnv = config.flake.lib.mkShellEnv shell;

            QUITSH_TOOLCHAINS = lib.concatStringsSep "," (shell.config.quitsh.toolchains or [ ]);
          }
          // derivationArgs;
        }
        ''
          source "$shellEnv"
          ${lib.optionalString enterShell ''eval "$shellHook"''}

          cd ${lib.escapeShellArg rootDir}

          # `stdenv` points HOME at /homeless-shelter, which breaks every tool
          # that wants a cache (`go build`, ...). NOTE: the builder runs as a
          # *nix build user*, so it can read the work tree but not write to it -
          # HOME must point somewhere that build user owns. Pass `homeDir` to
          # share a warm cache across dirty builds.
          export HOME="${homeDirExpr}"
          mkdir -p "$HOME"

          ${text}
        '';

    # Make a devenv shell (from `inputs.devenv`) with the following features:
    # - Using `pkgs` or imported from flake input `inputs.nixpkgs-devenv` for the `system` if not given.
    # - Additional `modules` added.
    # - Additional quitsh modules added.
    # - Allow unfree packages.
    # - Flake integration set to `true`
    # - Misc: If `inputs.devenv-root` is given then `devenv.root` is set to the flake input `inputs.devenv-root`
    #   defined by
    #
    #    ```nix
    #       devenv-root = {
    #         url = "file+file:///dev/null";
    #         flake = false;
    #       };
    #     ```
    #   and set with `nix develop --no-pure-eval --override-input devenv-root "path:.devenv/state/pwd"
    #   where `.devenv/state/pwd` is the current project root directory. This is a workaround to allow
    #   pure evaluation.
    mkShell =
      {
        inputs,
        modules ? [ ],
        pkgs ? null,
        system ? null,
      }:
      assert lib.assertMsg (lib.hasAttr "self" inputs) "Inputs must contain `self`.";
      assert lib.assertMsg (lib.hasAttr "devenv" inputs) "Inputs must contain `devenv`.";
      assert lib.assertMsg (lib.hasAttr "nixpkgs-devenv" inputs) "Inputs must contain `nixpkgs-devenv`.";
      let
        pkgsForDevenv =
          if pkgs == null then
            assert lib.assertMsg (system != null) "System must be given";
            import inputs.nixpkgs-devenv {
              config.allowUnfree = true;
              inherit system;
            }
          else
            pkgs;

        # Only inject what devenv really uses;
        ins = {
          inherit (inputs) self devenv;

          # Why nixpkgs is used is a bit weird.
          # See https://github.com/cachix/devenv/pull/2091
          nixpkgs =
            assert lib.assertMsg (system != null) "System must be given";
            inputs.nixpkgs-devenv;
        };

      in
      inputs.devenv.lib.mkShell {
        inputs = ins;
        pkgs = pkgsForDevenv;

        modules = [
          (
            {
              devenv.flakesIntegration = true;
            }
            # Only apply it if `devenv-root` is defined.
            // lib.optionalAttrs (lib.hasAttr "devenv-root" inputs) {
              # This is currently needed for devenv to properly run in pure hermetic
              # mode while still being able to run processes & services and modify
              # (some parts) of the active shell.
              # We read here the root for devenv from the workaround flake input `devenv-root`.
              devenv.root = lib.strings.trim (builtins.readFile inputs.devenv-root.outPath);
            }
          )
        ]
        ++ config.flake.lib.devenv.modules
        ++ modules;
      };
  };
}
