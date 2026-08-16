FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    perl libjson-perl libdigest-sha-perl libtext-csv-perl \
    luajit \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://julialang-s3.julialang.org/bin/linux/x64/1.11/julia-1.11.3-linux-x86_64.tar.gz && \
    tar xzf julia-1.11.3-linux-x86_64.tar.gz && \
    mv julia-1.11.3 /opt/julia && \
    ln -sf /opt/julia/bin/julia /usr/local/bin/julia && \
    rm julia-1.11.3-linux-x86_64.tar.gz

RUN julia -e 'using Pkg; Pkg.add(["CSV", "DataFrames", "Statistics", "JSON", "HTTP"])'

COPY zig/partyvault-crypto /usr/local/bin/partyvault-crypto
RUN chmod +x /usr/local/bin/partyvault-crypto

WORKDIR /app
COPY data/ ./data/
COPY perl/ ./perl/
COPY lua/ ./lua/
COPY julia/ ./julia/
COPY web/ ./web/
COPY run_demo.sh ./
RUN chmod +x run_demo.sh
RUN mkdir -p output

CMD ["./run_demo.sh"]
