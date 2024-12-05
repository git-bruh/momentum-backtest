#!/bin/sh

set -eu

DATA_DIR="$PWD/data/indices"

mkdir -p "$DATA_DIR"

cd "$DATA_DIR"

for year in 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024; do
    for month in Jan Feb Apr Mar May Jun Jul Aug Sep Oct Nov Dec; do
        axel "https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/indices_data${month}${year}.zip"
    done
done
