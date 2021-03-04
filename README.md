Connect to a kdb+ service using native rust.
Provides support for kdb+ connectivity using 
uncompressed serialization and deserialization, 
following the [Kx Documentation](https://code.kx.com/q/kb/serialization/).

## Features
* Written natively in Rust using stable features only
* Leverages Rust's type and enum system to match cleanly with the kdb+ type system
* Outputs `rsq::KObj` to kdb+ readable format i.e. ```(`TSLA;`Q;653.20;200)```
* Supports atomic types (0-19h), lists, dictionaries, and tables

## Drawbacks
Since `rsq` is written natively in Rust, it is capable of running
on any stable version of the language. This comes at the cost of
not using compression/decompression, which is only possible using the
 proprietary Kx provided `c.so`. Therefore, this library is primarily 
for applications where compression is not needed. This would include
feedhandlers, realtime consumers, etc. as kdb+ only compresses 
[under certain conditions](https://code.kx.com/q/basics/ipc/#compression)

## Usage
Put this in your `Cargo.toml`:
```toml
[dependencies]
rsq = "0.1"
```

## Example 
### Tickerplant Subscriber

The following code will subscribe to a vanilla tickerplant
for all symbols and print the realtime data to stdout
using the basic `println!` macro

```no_run
use rsq::{Kdb, KObj, KType};
let mut kdb = Kdb::new("localhost", 5001, "username", "password");

kdb.send_async(&KObj::List(vec![
    KObj::Atom(KType::Symbol(".u.sub".to_string())),
    KObj::Atom(KType::Symbol("trade".to_string())),
    KObj::Atom(KType::Symbol("".to_string()))
])).unwrap();

loop {
    println!("{}",kdb.read());
};
```
**Output**
```bash
(`upd;`trade;flip (`time;`sym;`price;`size)!((enlist 20:57:00.000);(enlist `TSLA);(enlist 653.1f);(enlist 50j)))
(`upd;`trade;flip (`time;`sym;`price;`size)!((enlist 20:59:00.000);(enlist `TSLA);(enlist 653.2f);(enlist 30j)))
(`upd;`trade;flip (`time;`sym;`price;`size)!((enlist 20:59:30.000);(enlist `TSLA);(enlist 653.1f);(enlist 100j)))
```