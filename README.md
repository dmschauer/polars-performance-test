# polars-performance-test

It's said that Polars runs faster in Python than in Rust, as long as 
a) you're not putting effort into setting optimal flags for the Rust compiler
b) you stay away from UDFs in Python

Source:
- https://github.com/pola-rs/polars/issues/8391
- https://github.com/pola-rs/polars/issues/9353
- https://docs.pola.rs/user-guide/expressions/user-defined-functions/

Let's see if that's true, and how big the difference really is.

Assuming you already have Python, pip and Rust installed

Run Python code:
```
cd python_polars
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python python_polars.py
```

Run Rust code:
```
cd rust_polars
cargo run --release
```

