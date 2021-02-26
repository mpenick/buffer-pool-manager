use crate::buffer_pool::{DiskManager, DiskManagerMock, MAX_NUM_DISK_PAGES, PageId, PageError, Page};
use crate::buffer_pool::PageError::{OutOfStorage, PageNotFound};
use std::any::Any;

impl DiskManager for DiskManagerMock {
    fn read_page(&mut self, id: PageId) -> Result<&Box<Page>, PageError> {
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
        if self.num_pages >= MAX_NUM_DISK_PAGES {
            return Err(OutOfStorage);
        }
        self.num_pages += 1;
        Ok(self.num_pages)
    }

    fn deallocate_page(&mut self, id: PageId) {
        self.pages.remove(&id);
    }

    fn pages_on_disk(&self) -> Vec<i32> {
        let mut pages: Vec<i32> = Vec::new();
        for id in self.pages.keys() {
            pages.push(*id);
        }
        pages.sort();
        pages
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

