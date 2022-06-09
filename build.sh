#!/bin/sh
set -eou pipefail
python src/build.py
Rscript src/figures/travel-history.r
Rscript src/figures/delay-to-confirmation.r
Rscript src/figures/genomics.r
Rscript src/figures/age-gender.r
