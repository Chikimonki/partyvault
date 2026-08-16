FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    perl libjson-perl libdigest-sha-perl libtext-csv-perl \
    luajit \
    ca-certificates \
    wget \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Zig 0.13.0
RUN wget -q https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz && \
    tar xf zig-linux-x86_64-0.13.0.tar.xz && \
    mv zig-linux-x86_64-0.13.0 /opt/zig && \
    ln -sf /opt/zig/zig /usr/local/bin/zig && \
    rm zig-linux-x86_64-0.13.0.tar.xz

# Install Julia
RUN wget -q https://julialang-s3.julialang.org/bin/linux/x64/1.11/julia-1.11.3-linux-x86_64.tar.gz && \
    tar xzf julia-1.11.3-linux-x86_64.tar.gz && \
    mv julia-1.11.3 /opt/julia && \
    ln -sf /opt/julia/bin/julia /usr/local/bin/julia && \
    rm julia-1.11.3-linux-x86_64.tar.gz

RUN julia -e 'using Pkg; Pkg.add(["CSV", "DataFrames", "Statistics", "JSON", "HTTP"])'

WORKDIR /app
COPY . .

# Build Zig crypto core
RUN cd zig && zig build

# Move binary to expected location
RUN cp zig/zig-out/bin/partyvault-crypto /usr/local/bin/partyvault-crypto

RUN mkdir -p output
RUN chmod +x run_demo.sh

CMD ["./run_demo.sh"]
