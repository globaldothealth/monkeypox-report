{ pkgs ? import <nixpkgs> {}
}:
pkgs.mkShell {
  buildInputs = with pkgs; [
    python310
    python310Packages.pandas
    python310Packages.requests
    python310Packages.boto3
    python310Packages.inflect
    python310Packages.chevron
    python310Packages.pyyaml

    R
    rPackages.cowplot
    rPackages.dplyr
    rPackages.ggplot2
    rPackages.ggpubr
    rPackages.rworldmap
    rPackages.styler
    rPackages.rjson
    rPackages.RColorBrewer
  ];

  shellHook = ''
    export LC_ALL=C.UTF-8
  '';
}
