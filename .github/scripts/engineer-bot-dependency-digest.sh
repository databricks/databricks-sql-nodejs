#!/usr/bin/env bash
set -euo pipefail

tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf - node_modules | sha256sum
