#!/bin/bash

echo "Starting GrinPool Stratum Server"
cp /usr/local/bin/grin-pool.toml /stratum/.grin-pool.toml
/usr/local/bin/grin-pool
