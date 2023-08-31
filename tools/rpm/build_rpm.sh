#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 /path/to/binary"
    exit 1
fi

# Get the full path of the binary
BINARY_PATH=$(realpath "$1")
echo "Preparing $BINARY_PATH"

# Get the basename to use as the package name
PKG_NAME=$(basename "$BINARY_PATH")

# Setup RPM build environment in a unique subdirectory under /tmp
RPM_ROOT=$(mktemp -d /tmp/rpmbuild_XXXXXX)
echo "Working dir is $RPM_ROOT"
mkdir -p $RPM_ROOT/{BUILD,RPMS,SOURCES,SPECS}

# Copy the binary to the SOURCES directory
cp "$BINARY_PATH" "$RPM_ROOT/SOURCES/"

# Generate a simple RPM spec file
cat <<EOF > "$RPM_ROOT/SPECS/$PKG_NAME.spec"
Name:       $PKG_NAME
Version:    1.0
Release:    1%{?dist}
Summary:    Auto generated package for $PKG_NAME

License:    APACHE 2.0
Source0:    $PKG_NAME

%description
Auto generated package for $PKG_NAME

%prep
%build

%install
mkdir -p %{buildroot}/usr/local/bin
install -m 755 %{SOURCE0} %{buildroot}/usr/local/bin/

%files
/usr/local/bin/$PKG_NAME

%changelog
EOF

# Build only the binary RPM package
rpmbuild --define "_topdir $RPM_ROOT" -bb "$RPM_ROOT/SPECS/$PKG_NAME.spec"

echo "RPM package built and located in $RPM_ROOT/RPMS/"
