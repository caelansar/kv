use crate::{CommandResponse, Storage};

mod command_service;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}
