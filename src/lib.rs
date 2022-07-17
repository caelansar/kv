mod error;
mod pb;
mod storage;

pub use error::KvError;
pub use pb::abi::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
