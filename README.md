# monkeypox-report

This repository contains code and templates to generate the briefing report at https://www.monkeypox.global.health

## Setup

Setup is using [poetry](https://python-poetry.org/) and [renv](https://rstudio.github.io/renv/). Python is used for the overall build process, while R is used to generate the figures.
You can install poetry from your package manager or by

    curl -sSL https://install.python-poetry.org | python3 -

followed by `poetry install` to install the dependencies.

For R setup, open R on the terminal and type

```r
install.packages("renv")
renv::restore()
```
which will use the `renv.lock` file to install the dependencies.

## Building the report

By default, the build process will fetch data from the
[globaldothealth/monkeypox](https://github.com/globaldothealth/monkeypox)
repository, and Nextstrain data from a private S3 bucket.Nextstrain data is
uploaded daily to the bucket by a [GitHub Action](.github/workflows/nextstrain.yml).

You can run the build pipeline using `poetry run python src/build.py`, this will use the [template](src/index.html), data files and Nextstrain data to update the variables for that day's report, which are written to [build/index.json](build/index.json).

To check differences, use `git diff`.

Once you are okay with the changes, commit and push to the `main` branch. The
[deploy](.github/workflows/deploy.yml) then deploys the latest report to S3.

In most cases, **manual report generation is not required**, as the
[build](.github/workflows/build.yml) action builds a report each working day
and opens a pull request for review.
