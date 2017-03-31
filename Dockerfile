FROM clux/muslrust:nightly

RUN cargo install cargo-fuzz 

ENV PATH=$PATH:/root/.cargo/bin
