mod buffer_pool;

use std::collections::HashMap;
use crate::buffer_pool::{FrameId, Replacer, PageId, DiskManager, Page, PageError, MAX_NUM_PAGES, BufferPoolManager};
use crate::buffer_pool::PageError::{NotFound, OutOfStorage};

// Why are hash map keys references?

struct ClockReplacer {
    list: Vec<(FrameId, bool)>,
    current: usize
}

impl ClockReplacer {
    fn new() -> ClockReplacer {
        ClockReplacer {
            list: Vec::new(),
            current: 0
        }
    }

    fn remove(&mut self, index: usize) {
        self.list.remove(index);
        if self.current >= self.list.len() {
            self.current = 0;
        }
    }
}

impl Replacer for ClockReplacer {
    fn victim(&mut self) -> Option<FrameId> {
        if self.list.is_empty() {
            return None
        }

        loop {
            if self.list[self.current].1 {
                self.list[self.current].1 = false;
                self.current = (self.current + 1) % self.list.len();
            } else {
                let frame_id = self.list[self.current].0;
                self.remove(self.current);
                return Some(frame_id)
            }
        }
    }

    fn unpin(&mut self, id: FrameId) {
        self.list.push((id, true));
    }

    fn pin(&mut self, id: FrameId) {
        match self.list.iter().position(|&e| e.0 == id) {
            Some(index) => self.remove(index),
            None => (),
        };
    }
}

struct DiskManagerMock {
    num_pages: u32,
    pages: HashMap<PageId, Box<Page>>,
}

impl DiskManagerMock {
    fn new() -> DiskManagerMock {
        DiskManagerMock {
            num_pages: 0,
            pages: HashMap::new(),
        }
    }
}

impl DiskManager for DiskManagerMock {
    fn read_page(&mut self, id: PageId) -> Result<&mut Page, PageError> {
        match self.pages.get_mut(&id) {
            Some(page) => {
                Ok(page)
            }
            None => Err(NotFound)
        }
    }

    fn write_page(&mut self, page: &Box<Page>) -> Result<(), PageError> {
        self.pages.insert(page.id(), page.clone());
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<PageId, PageError> {
        if self.num_pages >= MAX_NUM_PAGES {
            return Err(OutOfStorage);
        }
        self.num_pages += 1;
        Ok(self.num_pages)
    }

    fn deallocate_page(&mut self, id: PageId) {
        self.pages.remove(&id);
    }
}

fn main() {
    let mut dm = DiskManagerMock::new();
    let mut r = ClockReplacer::new();
    let mut bpm = BufferPoolManager::new(&mut dm, &mut r);

    let page_id: PageId;
    match bpm.new_page() {
        Ok(page) => {
            page.data[0] = 1;
        }
        _ => ()
    }

    bpm.flush_all_pages();

    println!("Hello, world!");
}