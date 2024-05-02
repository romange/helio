#!/bin/bash

containerWorkspaceFolder=$1
git config --global --add safe.directory ${containerWorkspaceFolder}
mkdir -p /root/.local/share/CMakeTools