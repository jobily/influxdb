#[derive(Debug, Clone, Copy)]
pub struct WalNoOp {}

impl WalNoOp {
    pub fn new() -> Self {
        Self {}
    }
}
