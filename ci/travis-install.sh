#!/bin/sh
set -e

echo "Configuring maven setting.xml"
mkdir -p ~/.m2
cp ci/settings.xml ~/.m2/settings.xml
