mod test_iter {
    use abyssiniandb::{DbBytes, DbInt, DbMap, DbMapKeyType, DbString, DbXxxBase};
    use std::collections::BTreeMap;
    //
    fn iter_test_map_empty_iter<T: DbMap<K>, K: DbMapKeyType>(db_map: &mut T) {
        assert_eq!(db_map.is_empty().unwrap(), true);
        //
        //assert_eq!(db_map.drain().next(), None);
        //assert_eq!(db_map.keys().next(), None);
        //assert_eq!(db_map.values().next(), None);
        //assert_eq!(db_map.values_mut().next(), None);
        assert_eq!(db_map.iter().next(), None);
        //assert_eq!(db_map.iter_mut().next(), None);
        //assert_eq!(db_map.len(), 0);
        //assert!(db_map.is_empty());
        //assert_eq!(db_map.into_iter().next(), None);
    }
    //
    fn basic_test_map_string<T: DbMap<DbString>>(db_map: &mut T) {
        // insert
        db_map.put_string("key01", "value1").unwrap();
        db_map.put_string("key02", "value2").unwrap();
        db_map.put_string("key03", "value3").unwrap();
        db_map.put_string("key04", "value4").unwrap();
        db_map.put_string("key05", "value5").unwrap();
        assert_eq!(db_map.len().unwrap(), 5);
        // iterator
        let btmap: BTreeMap<DbString, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some(("key01".into(), "value1".into())));
        assert_eq!(iter.next(), Some(("key02".into(), "value2".into())));
        assert_eq!(iter.next(), Some(("key03".into(), "value3".into())));
        assert_eq!(iter.next(), Some(("key04".into(), "value4".into())));
        assert_eq!(iter.next(), Some(("key05".into(), "value5".into())));
        assert_eq!(iter.next(), None);
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_string<T: DbMap<DbString>>(db_map: &mut T) {
        const LOOP_MAX: u64 = 100;
        // insert
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), LOOP_MAX);
        // iterator
        let btmap: BTreeMap<DbString, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        let btmap: BTreeMap<DbString, Vec<u8>> = db_map.iter_mut().collect();
        let iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        let mut i: i32 = 0;
        for (k, v) in iter {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        //
        //db_map.sync_data().unwrap();
    }
    fn basic_test_map_dbint<T: DbMap<DbInt>>(db_map: &mut T) {
        // insert
        db_map.put_string(&12301, "value1").unwrap();
        db_map.put_string(&12302, "value2").unwrap();
        db_map.put_string(&12303, "value3").unwrap();
        db_map.put_string(&12304, "value4").unwrap();
        db_map.put_string(&12305, "value5").unwrap();
        assert_eq!(db_map.len().unwrap(), 5);
        // iterator
        let btmap: BTreeMap<DbInt, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some((12301.into(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((12302.into(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some((12303.into(), b"value3".to_vec())));
        assert_eq!(iter.next(), Some((12304.into(), b"value4".to_vec())));
        assert_eq!(iter.next(), Some((12305.into(), b"value5".to_vec())));
        assert_eq!(iter.next(), None);
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_dbint<T: DbMap<DbInt>>(db_map: &mut T) {
        const LOOP_MAX: u64 = 100;
        // insert
        for i in 0..LOOP_MAX {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), LOOP_MAX);
        // iterator
        let btmap: BTreeMap<DbInt, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        let btmap: BTreeMap<DbInt, Vec<u8>> = db_map.iter_mut().collect();
        let iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        let mut i: i32 = 0;
        for (k, v) in iter {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        //
        //db_map.sync_data().unwrap();
    }
    fn basic_test_map_bytes<T: DbMap<DbBytes>>(db_map: &mut T) {
        // insert
        db_map.put_string(b"key01".into(), "value1").unwrap();
        db_map.put_string(b"key02".into(), "value2").unwrap();
        db_map.put_string(b"key03".into(), "value3").unwrap();
        db_map.put_string(b"key04".into(), "value4").unwrap();
        db_map.put_string(b"key05".into(), "value5").unwrap();
        assert_eq!(db_map.len().unwrap(), 5);
        // iterator
        let btmap: BTreeMap<DbBytes, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some((b"key01".into(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((b"key02".into(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some((b"key03".into(), b"value3".to_vec())));
        assert_eq!(iter.next(), Some((b"key04".into(), b"value4".to_vec())));
        assert_eq!(iter.next(), Some((b"key05".into(), b"value5".to_vec())));
        assert_eq!(iter.next(), None);
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_bytes<T: DbMap<DbBytes>>(db_map: &mut T) {
        const LOOP_MAX: u64 = 100;
        // insert
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), LOOP_MAX);
        // iterator
        let btmap: BTreeMap<DbBytes, Vec<u8>> = db_map.iter_mut().collect();
        let mut iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        let btmap: BTreeMap<DbBytes, Vec<u8>> = db_map.iter_mut().collect();
        let iter = btmap.into_iter();
        //let mut iter = db_map.iter_mut();
        let mut i: i32 = 0;
        for (k, v) in iter {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        //
        //db_map.sync_data().unwrap();
    }
    ////
    #[test]
    fn test_file_map_string() {
        let db_name = "target/tmp/test_iter-s.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_string("some_string_1").unwrap();
        //
        iter_test_map_empty_iter(&mut db_map);
        //
        basic_test_map_string(&mut db_map);
        medium_test_map_string(&mut db_map);
    }
    #[test]
    fn test_file_map_dbint() {
        let db_name = "target/tmp/test_iter-u.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        iter_test_map_empty_iter(&mut db_map);
        //
        basic_test_map_dbint(&mut db_map);
        medium_test_map_dbint(&mut db_map);
    }
    #[test]
    fn test_file_map_bytes() {
        let db_name = "target/tmp/test_iter-b.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_bytes("some_bytes_1").unwrap();
        //
        iter_test_map_empty_iter(&mut db_map);
        //
        basic_test_map_bytes(&mut db_map);
        medium_test_map_bytes(&mut db_map);
    }
    //
    #[test]
    fn test_iteration() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_iter-iteration.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        for i in 0..32 {
            db_map.put(&i, &[i as u8 * 2]).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), 32);
        //
        let mut observed: u32 = 0;
        for (k, v) in &db_map {
            let n: u64 = k.into();
            assert_eq!(v[0], n as u8 * 2);
            observed |= 1 << n;
        }
        assert_eq!(observed, 0xFFFF_FFFF);
    }
    #[test]
    fn test_keys() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_keys.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        db_map.put_string(&1, "a").unwrap();
        db_map.put_string(&2, "b").unwrap();
        db_map.put_string(&3, "c").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);
        //
        let keys: Vec<u64> = db_map.keys().map(|i| i.into()).collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&1));
        assert!(keys.contains(&2));
        assert!(keys.contains(&3));
    }
    #[test]
    fn test_values() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_values.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        db_map.put_string(&1, "a").unwrap();
        db_map.put_string(&2, "b").unwrap();
        db_map.put_string(&3, "c").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);
        //
        let values: Vec<String> = db_map
            .values()
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .collect();
        assert_eq!(values.len(), 3);
        assert!(values.contains(&"a".to_string()));
        assert!(values.contains(&"b".to_string()));
        assert!(values.contains(&"c".to_string()));
    }
    #[test]
    fn test_put_from_iter() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_put_from_iter.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        let xs = [(1, 1), (2, 2), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];
        db_map
            .put_from_iter(xs.iter().map(|&(k, v)| (k.into(), vec![v as u8])))
            .unwrap();
        assert_eq!(db_map.len().unwrap(), 6);
        //
        for &(k, v) in &xs {
            assert_eq!(db_map.get(&k).unwrap(), Some(vec![v as u8]));
        }
        assert_eq!(db_map.len().unwrap() as usize, xs.len() - 1);
    }
    #[test]
    fn test_size_hint() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_size_hint.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        let xs = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];
        db_map
            .put_from_iter(xs.iter().map(|&(k, v)| (k.into(), vec![v as u8])))
            .unwrap();
        assert_eq!(db_map.len().unwrap(), 6);
        //
        let mut iter = db_map.iter();
        for _ in iter.by_ref().take(3) {}
        assert_eq!(iter.size_hint(), (3, Some(3)));
    }
    #[test]
    fn test_iter_len() {
        use abyssiniandb::DbXxx;
        //
        let db_name = "target/tmp/test_iter_len.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        //
        let xs = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];
        db_map
            .put_from_iter(xs.iter().map(|&(k, v)| (k.into(), vec![v as u8])))
            .unwrap();
        assert_eq!(db_map.len().unwrap(), 6);
        //
        let mut iter = db_map.iter();
        for _ in iter.by_ref().take(3) {}
        assert_eq!(iter.len(), 3);
    }
}
