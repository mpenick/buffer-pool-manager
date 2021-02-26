# Buffer Pool in Rust

Port of this excellent article: https://brunocalza.me/how-buffer-pool-works-an-implementation-in-go/

Warning: This is my first ever attempt at writing Rust. 

The resulting API doesn't seem very Rust idiomatic. I might follow up on this.

# How to build and run

```
cargo run
```

# Use the Javascript app from the original buffer-pool-manager project

```
git submodule update --init --recursive
pushd buffer-pool-manager/web/dist/ && python -m SimpleHTTPServer; popd
```
