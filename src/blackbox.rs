///! Defines types and implementations for a Black Box Recorder (BBR) which captures in a
///! structured way operations performed within the structure to make it easier to test and debug the
///! structure.
use std::fmt::{Debug, Display};
use std::string::ToString;

pub trait Operation {
    fn op_name(&self) -> &'static str;

    fn op_detail(&self) -> String;
}

impl<T: Display + Debug> Operation for T {
    fn op_name(&self) -> &'static str {
        self.to_string()
    }
    fn op_detail(&self) -> String {
        format!("{:?}", self)
    }
}

pub struct OpRecord {
    indent: usize,
    op_name: &'static str,
    op_detail: String,
}

pub trait BlackBoxRecorder {
    fn record<O: Operation>(&mut self, op: O) -> ();
}

pub struct DisabledRecorder {}

impl BlackBoxRecorder for DisabledRecorder {
    fn record<O: Operation>(&mut self, op: O) -> () {}
}
