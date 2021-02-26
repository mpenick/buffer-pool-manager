use std::fmt::{Display, Formatter};
use std::fmt;
use crate::buffer_pool::{PageId, Page, PAGE_SIZE};

impl Page {
    pub fn new(id: PageId) -> Box<Page> {

        Box::new(Page {
            id,
            pin_count: 1,
            is_dirty: false,
            data: [0; PAGE_SIZE],
        })
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    pub fn dec_pin_count(&mut self) -> bool {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        self.pin_count == 0
    }
}

#[derive(Debug, PartialEq)]
pub enum PageError {
    PageNotFound,
    PageStillInUse,
    PoolExhausted,
    OutOfStorage,
}

impl Display for PageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
