use crate::buffer_pool::PageError::{PageNotFound, PageSillInUse, PoolExhausted};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::{Display, Formatter};

pub const MAX_POOL_SIZE: usize = 256;
pub const MAX_NUM_PAGES: u32 = 256;
pub const PAGE_SIZE: usize = 8192;

pub type FrameId = u32;
pub type PageId = u32;

#[derive(Copy, Clone)]
pub struct Page {
    id: PageId,
    pin_count: i32,
    is_dirty: bool,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    fn new(id: PageId) -> Box<Page> {
        Box::new(Page {
            id,
            pin_count: 0,
            is_dirty: false,
            data: [0; PAGE_SIZE],
        })
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    fn dec_pin_count(&mut self) -> bool {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
        self.pin_count == 0
    }
}

#[derive(Debug)]
pub enum PageError {
    PageNotFound,
    PageSillInUse,
    PoolExhausted,
    OutOfStorage,
}

impl Display for PageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub trait Replacer {
    fn victim(&mut self) -> Option<FrameId>;
    fn unpin(&mut self, id: FrameId);
    fn pin(&mut self, id: FrameId);
}

pub trait DiskManager {
    fn read_page(&mut self, id: PageId) -> Result<&mut Page, PageError>;
    fn write_page(&mut self, page: &Box<Page>) -> Result<(), PageError>;
    fn allocate_page(&mut self) -> Result<PageId, PageError>;
    fn deallocate_page(&mut self, id: PageId);
}

pub struct BufferPoolManager<'a> {
    disk_manager: &'a mut dyn DiskManager,
    replacer: &'a mut dyn Replacer,
    pages: Vec<Option<Box<Page>>>,
    free_list: VecDeque<FrameId>,
    page_table: HashMap<PageId, FrameId>,
}

impl<'a> BufferPoolManager<'a> {
    pub fn new(
        disk_manager: &'a mut dyn DiskManager,
        replacer: &'a mut dyn Replacer,
    ) -> BufferPoolManager<'a> {
        let mut manager = BufferPoolManager {
            disk_manager,
            replacer,
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
                            page.pin_count = 1;
                            self.page_table.insert(id, frame_id);
                            Ok(page)
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
                page.dec_pin_count();
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
                page.dec_pin_count();
                if let Err(e) = self.disk_manager.write_page(page) {
                    return Err(e);
                }
                page.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn delete_page(&mut self, id: PageId) -> Result<(), PageError> {
        if let Some(frame_id) = self.page_table.remove(&id) {
            if let Some(page) = self.pages[frame_id as usize].as_mut() {
                if page.pin_count > 0 {
                    return Err(PageSillInUse);
                }
                self.replacer.pin(frame_id);
                self.disk_manager.deallocate_page(id);
                self.free_list.push_back(frame_id);
            } else {
                panic!("not possible!")
            }
            Ok(())
        } else {
            Err(PageNotFound)
        }
    }

    pub fn response(&self) -> Response {
        let mut response = Response {
            pages_in_disk: vec![],
            max_pool_size: 0,
            page_table: Default::default(),
            max_disk_num_pages: 0,
            pin_count: Default::default(),
        };
        response
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
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
pub struct Response {
    #[serde(rename = "PagesInDisk")]
    pub pages_in_disk: Vec<PageId>,
    #[serde(rename = "MaxPoolSize")]
    pub max_pool_size: usize,
    #[serde(rename = "PagesTable")]
    pub page_table: HashMap<PageId, FrameId>,
    #[serde(rename = "MaxDiskNumPages")]
    pub max_disk_num_pages: usize,
    #[serde(rename = "PinCount")]
    pub pin_count: HashMap<i32, i32>,
}
