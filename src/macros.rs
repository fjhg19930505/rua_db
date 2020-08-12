#![macro_use]

macro_rules! unwrap_or {
    ($expr:expr, $or:expr) => (
        match $expr {
            Some(x) => x,
            None => { $or }
        }
    )
}

macro_rules! ok_or {
    ($expr: expr, $or: expr) => (
        match $expr {
            Ok(x) => x,
            Err(_) => { $or }
        }
    )
}

macro_rules! fail {
    ($expr:expr) => (
        return Err(::std::convert::From::from($expr));
    )
}