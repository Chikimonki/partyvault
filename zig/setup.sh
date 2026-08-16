#!/bin/bash
set -euo pipefail

echo "============================================"
echo " PartyVault — Cryptographic Party Identity"
echo " Setup for WSL (Ubuntu/Debian)"
echo "============================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

check_install() {
    if command -v "$1" &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} $1 found: $(command -v $1)"
        return 0
    else
        echo -e "  ${RED}✗${NC} $1 not found"
        return 1
    fi
}

echo "--- Checking existing installations ---"
echo ""

NEED_ZIG=0
NEED_LUAJIT=0
NEED_JULIA=0
NEED_PERL=0

check_install "zig" || NEED_ZIG=1
check_install "luajit" || NEED_LUAJIT=1
check_install "julia" || NEED_JULIA=1
check_install "perl" || NEED_PERL=1

echo ""
echo "--- Installing missing components ---"
echo ""

sudo apt-get update -qq

# Perl (almost certainly already on the system)
if [ $NEED_PERL -eq 1 ]; then
    echo -e "${YELLOW}Installing Perl...${NC}"
    sudo apt-get install -y perl libdigest-sha-perl libjson-perl libtext-csv-perl
else
    echo "Ensuring Perl modules..."
    sudo apt-get install -y libdigest-sha-perl libjson-perl libtext-csv-perl 2>/dev/null || \
        echo "  (install cpanminus if modules missing: sudo cpan JSON Text::CSV)"
fi

# LuaJIT
if [ $NEED_LUAJIT -eq 1 ]; then
    echo -e "${YELLOW}Installing LuaJIT...${NC}"
    sudo apt-get install -y luajit
fi

# Julia
if [ $NEED_JULIA -eq 1 ]; then
    echo -e "${YELLOW}Installing Julia...${NC}"
    # Use juliaup for latest, or apt for simplicity
    if command -v snap &> /dev/null; then
        sudo snap install julia --classic 2>/dev/null || {
            echo "Snap failed, trying manual install..."
            JULIA_VERSION="1.11.3"
            wget -q "https://julialang-s3.julialang.org/bin/linux/x64/1.11/julia-${JULIA_VERSION}-linux-x86_64.tar.gz"
            tar xzf "julia-${JULIA_VERSION}-linux-x86_64.tar.gz"
            sudo mv "julia-${JULIA_VERSION}" /opt/julia
            sudo ln -sf /opt/julia/bin/julia /usr/local/bin/julia
            rm "julia-${JULIA_VERSION}-linux-x86_64.tar.gz"
        }
    else
        sudo apt-get install -y julia 2>/dev/null || {
            echo -e "${RED}Please install Julia manually: https://julialang.org/downloads/${NC}"
        }
    fi
fi

# Zig
if [ $NEED_ZIG -eq 1 ]; then
    echo -e "${YELLOW}Installing Zig...${NC}"
    ZIG_VERSION="0.13.0"
    wget -q "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz"
    tar xf "zig-linux-x86_64-${ZIG_VERSION}.tar.xz"
    sudo mv "zig-linux-x86_64-${ZIG_VERSION}" /opt/zig
    sudo ln -sf /opt/zig/zig /usr/local/bin/zig
    rm "zig-linux-x86_64-${ZIG_VERSION}.tar.xz"
fi

# Install Julia packages
echo ""
echo "--- Installing Julia packages ---"
julia -e '
    using Pkg
    Pkg.add(["CSV", "DataFrames", "Statistics", "JSON"])
    println("Julia packages installed.")
' 2>/dev/null || echo "Julia packages will install on first run."

# Create output directory
mkdir -p output

echo ""
echo "============================================"
echo -e " ${GREEN}Setup complete!${NC}"
echo ""
echo " Versions:"
zig version 2>/dev/null || echo "  zig: not found"
luajit -v 2>/dev/null || echo "  luajit: not found"  
julia --version 2>/dev/null || echo "  julia: not found"
perl --version | head -2 2>/dev/null || echo "  perl: not found"
echo "============================================"
echo ""
echo "Run the demo:  ./run_demo.sh"
