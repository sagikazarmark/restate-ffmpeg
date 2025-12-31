{ pkgs, ... }:

{
  cachix.pull = [ "sagikazarmark-dev" ];

  dotenv.enable = true;

  packages = with pkgs; [
    cargo-release
    cargo-watch
    ffmpeg
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };
}
