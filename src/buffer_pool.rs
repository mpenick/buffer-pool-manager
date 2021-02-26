mod disk_manager_mock;
mod clock_replacer;
mod page;

use crate::buffer_pool::PageError::{PageNotFound, PageStillInUse, PoolExhausted};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use crate::buffer_pool::page::PageError;
use crate::buffer_pool::clock_replacer::{ClockReplacer, ClockReplacerRep};
use std::any::Any;

pub const MAX_POOL_SIZE: usize = 4;
pub const MAX_NUM_DISK_PAGES: i32 = 15;
pub const PAGE_SIZE: usize = 8;

pub type FrameId = i32;
pub type PageId = i32;

#[derive(Copy, Clone, Debug)]
pub struct Page {
    id: PageId,
    pin_count: i32,
    is_dirty: bool,
    pub data: [u8; PAGE_SIZE],
}

pub trait Replacer {
    fn victim(&mut self) -> Option<FrameId>;
    fn unpin(&mut self, id: FrameId);
    fn pin(&mut self, id: FrameId);
}

pub trait DiskManager {
    fn read_page(&mut self, id: PageId) -> Result<&Box<Page>, PageError>;
    fn write_page(&mut self, page: &Box<Page>) -> Result<(), PageError>;
    fn allocate_page(&mut self) -> Result<PageId, PageError>;
    fn deallocate_page(&mut self, id: PageId);
    fn pages_on_disk(&self) -> Vec<i32>;
    fn as_any(&self) -> &dyn Any;
}

pub struct DiskManagerMock {
    num_pages: i32,
    pages: HashMap<PageId, Box<Page>>,
}

impl DiskManagerMock {
    pub fn new() -> Box<DiskManagerMock> {
        Box::new(DiskManagerMock {
            num_pages: 0,
            pages: HashMap::new(),
        })
    }
}

pub struct BufferPoolManager {
    disk_manager: Box<dyn DiskManager + Send>,
    replacer: ClockReplacer,
    pages: Vec<Option<Box<Page>>>,
    free_list: VecDeque<FrameId>,
    page_table: HashMap<PageId, FrameId>,
}

impl BufferPoolManager {
    pub fn new(disk_manager: Box<dyn DiskManager + Send>) -> BufferPoolManager {
        let mut manager = BufferPoolManager {
            disk_manager,
            replacer: ClockReplacer::new(),
            pages: vec![None; MAX_POOL_SIZE],
            free_list: VecDeque::new(),
            page_table: HashMap::new(),
        };
        for i in 0..MAX_POOL_SIZE {
            manager.free_list.push_back(i as FrameId);
        }
        manager
    }

    pub fn new_page(&mut self) -> Result<&mut Page, PageError> {
        match self.get_frame_id() {
            Ok((frame_id, is_from_free_list)) => {
                if !is_from_free_list {
                    if let Err(e) = self.write_if_dirty(frame_id) {
                        return Err(e);
                    }
                }
                match self.disk_manager.allocate_page() {
                    Ok(page_id) => {
                        self.page_table.insert(page_id, frame_id);
                        self.pages[frame_id as usize] = Some(Page::new(page_id));
                        if let Some(page) = self.pages[frame_id as usize].as_mut() {
                            Ok(page)
                        } else {
                            panic!("not possible!")
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn fetch_page(&mut self, id: PageId) -> Result<&mut Page, PageError> {
        if let Some(frame_id) = self.page_table.get(&id) {
            if let Some(page) = self.pages[*frame_id as usize].as_mut() {
                page.pin_count += 1;
                self.replacer.pin(*frame_id);
                Ok(page)
            } else {
                panic!("not possible!")
            }
        } else {
            match self.get_frame_id() {
                Ok((frame_id, is_from_free_list)) => {
                    if !is_from_free_list {
                        if let Err(e) = self.write_if_dirty(frame_id) {
                            return Err(e);
                        }
                    }
                    match self.disk_manager.read_page(id) {
                        Ok(page) => {
                            self.page_table.insert(id, frame_id);
                            self.pages[frame_id as usize] = Some(page.clone());
                            if let Some(page) = self.pages[frame_id as usize].as_mut() {
                                page.pin_count = 1;
                                Ok(page)
                            } else {
                                panic!("not possible!")
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        }
    }

    pub fn unpin_page(&mut self, id: PageId, is_dirty: bool) -> Result<(), PageError> {
        if let Some(frame_id) = self.page_table.get(&id) {
            if let Some(page) = self.pages[*frame_id as usize].as_mut() {
                if page.dec_pin_count() {
                    self.replacer.unpin(*frame_id);
                }
                page.is_dirty = page.is_dirty || is_dirty;
            } else {
                panic!("not possible!")
            }
            Ok(())
        } else {
            Err(PageNotFound)
        }
    }

    pub fn flush_page(&mut self, id: PageId) -> Result<(), PageError> {
        if let Some(frame_id) = self.page_table.get(&id) {
            if let Some(page) = self.pages[*frame_id as usize].as_mut() {
                // page.dec_pin_count(); // In the original, but it might be a defect?
                if let Err(e) = self.disk_manager.write_page(page) {
                    return Err(e);
                }
                page.is_dirty = false;
            } else {
                panic!("not possible!")
            }
            Ok(())
        } else {
            Err(PageNotFound)
        }
    }

    pub fn flush_all_pages(&mut self) -> Result<(), PageError> {
        for maybe_page in self.pages.iter_mut() {
            if let Some(page) = maybe_page {
                // page.dec_pin_count(); // In the original, but it might be a defect?
                if let Err(e) = self.disk_manager.write_page(page) {
                    return Err(e);
                }
                page.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn delete_page(&mut self, id: PageId) -> Result<(), PageError> {
        if let Some(frame_id) = self.page_table.get(&id) {
            if let Some(page) = self.pages[*frame_id as usize].as_mut() {
                if page.pin_count > 0 {
                    return Err(PageStillInUse);
                }
                self.replacer.pin(*frame_id);
                self.disk_manager.deallocate_page(id);
                self.free_list.push_back(*frame_id);

                self.page_table.remove(&id);
            } else {
                panic!("not possible!")
            }
            Ok(())
        } else {
            Err(PageNotFound)
        }
    }

    fn get_frame_id(&mut self) -> Result<(FrameId, bool), PageError> {
        if !self.free_list.is_empty() {
            if let Some(frame_id) = self.free_list.pop_front() {
                Ok((frame_id, true))
            } else {
                panic!("not possible!")
            }
        } else {
            if let Some(frame_id) = self.replacer.victim() {
                Ok((frame_id, false))
            } else {
                Err(PoolExhausted)
            }
        }
    }

    fn write_if_dirty(&mut self, frame_id: FrameId) -> Result<(), PageError> {
        let mut existing_page: Option<Box<Page>> = None;
        std::mem::swap(&mut self.pages[frame_id as usize], &mut existing_page);
        if let Some(page) = existing_page {
            if page.is_dirty {
                return self.disk_manager.write_page(&page);
            }
            self.page_table.remove(&page.id);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
pub struct Response {
    #[serde(rename = "PagesInDisk")]
    pub pages_in_disk: Vec<PageId>,
    #[serde(rename = "MaxPoolSize")]
    pub max_pool_size: i32,
    #[serde(rename = "PagesTable")]
    pub page_table: HashMap<PageId, FrameId>,
    #[serde(rename = "ClockReplacer")]
    pub clock_replacer: ClockReplacerRep,
    #[serde(rename = "MaxDiskNumPages")]
    pub max_disk_num_pages: i32,
    #[serde(rename = "PinCount")]
    pub pin_count: HashMap<i32, i32>,
}

impl BufferPoolManager {
    pub fn response(&self) -> Response {
        let mut pin_count: HashMap<PageId, i32> = HashMap::new();
        for page in self.pages.iter() {
            if let Some(page) = page {
                pin_count.insert(page.id, page.pin_count);
            }
        }
        Response {
            pages_in_disk: self.disk_manager.pages_on_disk(),
            max_pool_size: MAX_POOL_SIZE as i32,
            page_table: self.page_table.clone(),
            clock_replacer: self.replacer.response(),
            max_disk_num_pages: MAX_NUM_DISK_PAGES,
            pin_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::{BufferPoolManager, DiskManagerMock, MAX_POOL_SIZE, DiskManager};
    use crate::buffer_pool::page::PageError::PoolExhausted;

    #[test]
    fn unpin_page() {
        let mut bpm = BufferPoolManager::new(DiskManagerMock::new());

        all_pages(&mut bpm);
        bpm.unpin_page(1, false).unwrap();
        assert_eq!(0, bpm.pages[0].as_ref().unwrap().pin_count);

        let page_id = bpm.new_page().unwrap().id;
        assert_eq!(MAX_POOL_SIZE as i32 + 1, page_id);
        assert_eq!(0, *bpm.page_table.get(&(MAX_POOL_SIZE as i32 + 1)).unwrap());

        assert_eq!(PoolExhausted, bpm.new_page().unwrap_err());
    }

    #[test]
    fn flush_page() {
        let mut bpm = BufferPoolManager::new(DiskManagerMock::new());

       assert_eq!(0, as_mock(&bpm.disk_manager).pages.len());

        let page_id = bpm.new_page().unwrap().id;
        bpm.flush_page(page_id).unwrap();

        assert!(as_mock(&bpm.disk_manager).pages.contains_key(&page_id))
    }

    #[test]
    fn fetch_page() {
        let mut bpm = BufferPoolManager::new(DiskManagerMock::new());

        all_pages(&mut bpm);
        bpm.unpin_page(1, false).unwrap();
        assert_eq!(1, bpm.pages[0].as_ref().unwrap().id);
        assert_eq!(0, bpm.pages[0].as_ref().unwrap().pin_count);
        bpm.flush_page(1).unwrap();

        bpm.new_page().unwrap();
        bpm.unpin_page(5, false).unwrap();
        assert_eq!(5, bpm.pages[0].as_ref().unwrap().id);
        assert_eq!(0, bpm.pages[0].as_ref().unwrap().pin_count);

        bpm.fetch_page(1).unwrap();
        assert_eq!(1, bpm.pages[0].as_ref().unwrap().id);
        assert_eq!(1, bpm.pages[0].as_ref().unwrap().pin_count);
    }

    #[test]
    fn delete_page() {
        let mut bpm = BufferPoolManager::new(DiskManagerMock::new());

        bpm.new_page().unwrap();
        assert!(bpm.page_table.contains_key(&1));

        bpm.flush_page(1).unwrap();
        assert!(as_mock(&bpm.disk_manager).pages.contains_key(&1));

        bpm.unpin_page(1, false).unwrap();

        bpm.delete_page(1).unwrap();
        assert!(!bpm.page_table.contains_key(&1));
        assert!(!as_mock(&bpm.disk_manager).pages.contains_key(&1));
    }

    #[test]
    fn flush_all_pages() {
        let mut bpm = BufferPoolManager::new(DiskManagerMock::new());

        all_pages(&mut bpm);
        assert_eq!(0, as_mock(&bpm.disk_manager).pages.len());

        bpm.flush_all_pages().unwrap();

        for i in 0..MAX_POOL_SIZE as i32 {
            assert!(as_mock(&bpm.disk_manager).pages.contains_key(&(i + 1)));
        }
    }

    fn as_mock(dm: &Box<dyn DiskManager + Send>) -> &DiskManagerMock {
        dm.as_any().downcast_ref::<DiskManagerMock>().unwrap()
    }

    fn all_pages(bpm: &mut BufferPoolManager) {
        for i in 0..MAX_POOL_SIZE as i32 {
            let page = bpm.new_page().unwrap();
            assert_eq!(i + 1, page.id());
            assert_eq!(i, *bpm.page_table.get(&(i + 1)).unwrap())
        }
    }
}
