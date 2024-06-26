use super::super::{DbMap, DbMapKeyType, DbXxx, DbXxxBase, DbXxxObjectSafe};
use super::{CheckFileDbMap, CountOfPerSize, KeysCountStats, LengthStats, RecordSizeStats};
use super::{DbXxxIntoIter, DbXxxIter, DbXxxIterMut, DbXxxKeys, DbXxxValues};
use super::{FileDbParams, FileDbXxxInner, Key, Value};
use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::rc::Rc;

pub mod kt_dbbytes;
pub mod kt_dbstring;
pub use kt_dbbytes::{DbBytes, FileDbMapDbBytes};
pub use kt_dbstring::{DbString, FileDbMapDbString};

pub mod kt_dbi64;
pub mod kt_dbu64;
pub use kt_dbi64::{DbI64, FileDbMapDbI64};
pub use kt_dbu64::{DbU64, FileDbMapDbU64};

pub mod kt_dbvu64;
pub use kt_dbvu64::{DbVu64, FileDbMapDbVu64};

/// DbMap in a file database.
#[derive(Debug, Clone)]
pub struct FileDbMap<KT: DbMapKeyType>(Rc<RefCell<FileDbXxxInner<KT>>>);

impl<KT: DbMapKeyType> FileDbMap<KT> {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMap<KT>> {
        Ok(Self(Rc::new(RefCell::new(
            FileDbXxxInner::<KT>::open_with_params(path, ks_name, params)?,
        ))))
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        RefCell::borrow(&self.0).is_dirty()
    }
}

/// for debug
impl<KT: DbMapKeyType + std::fmt::Display> CheckFileDbMap for FileDbMap<KT> {
    /*
    #[cfg(feature = "htx")]
    fn ht_size_and_count(&self) -> Result<(u64, u64)> {
        RefCell::borrow(&self.0).ht_size_and_count()
    }
    */
    /*
    /// convert the index node tree to graph string for debug.
    fn graph_string(&self) -> Result<String> {
        RefCell::borrow(&self.0).graph_string()
    }
    /// convert the index node tree to graph string for debug.
    fn graph_string_with_key_string(&self) -> Result<String> {
        RefCell::borrow_mut(&self.0).graph_string_with_key_string()
    }
    /// check the index node tree is balanced
    fn is_balanced(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_balanced()
    }
    /// check the index node tree is multi search tree
    fn is_mst_valid(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_mst_valid()
    }
    /// check the index node except the root and leaves of the tree has branches of hm or more.
    fn is_dense(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_dense()
    }
    /// get the depth of the index node.
    fn depth_of_node_tree(&self) -> Result<u64> {
        RefCell::borrow(&self.0).depth_of_node_tree()
    }
    /// count of the free node
    fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_node()
    }
    /// count of the used piece and the used node
    fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize, CountOfPerSize)> {
        RefCell::borrow(&self.0).count_of_used_node()
    }
    */
    /// count of the free key piece
    fn count_of_free_key_piece(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_key_piece()
    }
    /// count of the free value piece
    fn count_of_free_value_piece(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_value_piece()
    }
    /// buffer statistics
    #[cfg(feature = "rabuf_stats")]
    fn buf_stats(&self) -> Vec<(String, i64)> {
        RefCell::borrow(&self.0).buf_stats()
    }
    /// key piece size statistics
    fn key_piece_size_stats(&self) -> Result<RecordSizeStats<Key>> {
        RefCell::borrow(&self.0).key_piece_size_stats()
    }
    /// value piece size statistics
    fn value_piece_size_stats(&self) -> Result<RecordSizeStats<Value>> {
        RefCell::borrow(&self.0).value_piece_size_stats()
    }
    /// keys count statistics
    fn keys_count_stats(&self) -> Result<KeysCountStats> {
        RefCell::borrow(&self.0).keys_count_stats()
    }
    /// key length statistics
    fn key_length_stats(&self) -> Result<LengthStats<Key>> {
        RefCell::borrow(&self.0).key_length_stats()
    }
    /// value length statistics
    fn value_length_stats(&self) -> Result<LengthStats<Value>> {
        RefCell::borrow(&self.0).value_length_stats()
    }
    //#[cfg(feature = "htx")]
    fn htx_filling_rate_per_mill(&self) -> Result<(u64, u32)> {
        RefCell::borrow(&self.0).htx_filling_rate_per_mill()
    }
}

impl<KT: DbMapKeyType> DbXxxBase for FileDbMap<KT> {
    #[inline]
    fn len(&self) -> Result<u64> {
        RefCell::borrow(&self.0).len()
    }
    #[inline]
    fn read_fill_buffer(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).read_fill_buffer()
    }
    #[inline]
    fn flush(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).flush()
    }
    #[inline]
    fn sync_all(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_all()
    }
    #[inline]
    fn sync_data(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_data()
    }
}

impl<KT: DbMapKeyType> DbXxxObjectSafe<KT> for FileDbMap<KT> {
    #[inline]
    fn get_kt(&mut self, key: &KT) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).get_kt(key)
    }
    #[inline]
    fn put_kt(&mut self, key: &KT, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put_kt(key, value)
    }
    #[inline]
    fn del_kt(&mut self, key: &KT) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).del_kt(key)
    }
    #[inline]
    fn includes_key_kt(&mut self, key: &KT) -> Result<bool> {
        RefCell::borrow_mut(&self.0).includes_key_kt(key)
    }
}

impl<KT: DbMapKeyType> DbXxx<KT> for FileDbMap<KT> {}

impl<KT: DbMapKeyType> DbMap<KT> for FileDbMap<KT> {
    #[inline]
    fn iter(&self) -> DbXxxIter<KT> {
        DbXxxIter::new(self.0.clone()).unwrap()
    }
    #[inline]
    fn iter_mut(&mut self) -> DbXxxIterMut<KT> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
    #[inline]
    fn keys(&self) -> DbXxxKeys<KT> {
        DbXxxKeys::new(self.0.clone()).unwrap()
    }
    #[inline]
    fn values(&self) -> DbXxxValues<KT> {
        DbXxxValues::new(self.0.clone()).unwrap()
    }
}

// impl trait: IntoIterator
impl<KT: DbMapKeyType> IntoIterator for FileDbMap<KT> {
    type Item = (KT, Vec<u8>);
    type IntoIter = DbXxxIntoIter<KT>;
    //
    #[inline]
    fn into_iter(self) -> DbXxxIntoIter<KT> {
        DbXxxIntoIter::new(self.0).unwrap()
    }
}

impl<KT: DbMapKeyType> IntoIterator for &FileDbMap<KT> {
    type Item = (KT, Vec<u8>);
    type IntoIter = DbXxxIter<KT>;
    //
    #[inline]
    fn into_iter(self) -> DbXxxIter<KT> {
        DbXxxIter::new(self.0.clone()).unwrap()
    }
}

impl<KT: DbMapKeyType> IntoIterator for &mut FileDbMap<KT> {
    type Item = (KT, Vec<u8>);
    type IntoIter = DbXxxIterMut<KT>;
    //
    #[inline]
    fn into_iter(self) -> DbXxxIterMut<KT> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
}
