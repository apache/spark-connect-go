{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    (pkgs.spark.overrideAttrs (oldAttrs: {
      version = "3.5.1";
    }))

    (pkgs.protobuf.overrideAttrs (oldAttrs: {
      version = "24.4"; 
    }))
  ];

  # https://devenv.sh/languages/
  languages.java.enable = true;
  languages.java.jdk.package = pkgs.jdk11;
  languages.go.enable = true;

  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/scripts/
  scripts = {
    hello.exec = ''
      echo hello from $GREET
    '';
    run-spark-connect.exec = ''
      sudo start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.1
    '';
  };

  enterShell = ''
    hello
    spark-shell --version
    run-spark-connect
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
