mod buffer_pool;

use crate::buffer_pool::PageError::{OutOfStorage, PageNotFound};
use crate::buffer_pool::{
    BufferPoolManager, DiskManager, FrameId, Page, PageError, PageId, Replacer, MAX_NUM_PAGES,
};
use std::collections::HashMap;

// Why are hash map keys references?

struct ClockReplacer {
    list: Vec<(FrameId, bool)>,
    current: usize,
}

impl ClockReplacer {
    fn new() -> ClockReplacer {
        ClockReplacer {
            list: Vec::new(),
            current: 0,
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
            return None;
        }

        loop {
            if self.list[self.current].1 {
                self.list[self.current].1 = false;
                self.current = (self.current + 1) % self.list.len();
            } else {
                let frame_id = self.list[self.current].0;
                self.remove(self.current);
                return Some(frame_id);
            }
        }
    }

    fn unpin(&mut self, id: FrameId) {
        self.list.push((id, true));
    }

    fn pin(&mut self, id: FrameId) {
        if let Some(index) = self.list.iter().position(|&e| e.0 == id) {
            self.remove(index);
        }
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
        if let Some(page) = self.pages.get_mut(&id) {
            Ok(page)
        } else {
            Err(PageNotFound)
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

fn panic_if_error(result: Result<(), PageError>) {
    if let Err(e) = result {
        panic!("An error occurred {}", e)
    }
}

fn main() {
    let mut disk_manager = DiskManagerMock::new();
    let mut replacer = ClockReplacer::new();
    let mut manager =
        BufferPoolManager::new(&mut disk_manager, &mut replacer);

    let mut maybe_page_id: Option<PageId>;

    if let Ok(page) = manager.new_page() {
        page.data[0] = 1;
        maybe_page_id = Some(page.id())
    } else {
        panic!("Unable to allocate new page")
    }

    if let Some(page_id) = maybe_page_id {
        panic_if_error(manager.flush_page(page_id));
    }

    if let Ok(page) = manager.fetch_page(1) {
        page.data[0] = 2;
        maybe_page_id = Some(page.id())
    } else {
        panic!("Unable to fetch page")
    }

    if let Some(page_id) = maybe_page_id {
        panic_if_error(manager.unpin_page(page_id, true));
        panic_if_error(manager.delete_page(page_id));
    }

    panic_if_error(manager.flush_all_pages());
}
