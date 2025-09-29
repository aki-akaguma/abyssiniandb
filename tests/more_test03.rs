//
// Extended tests for DbBytes functionality
//
mod test_dbbytes_extended {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbMap, DbXxx, DbXxxBase, DbMapKeyType};
    use std::collections::HashSet;

    #[test]
    fn test_put_get_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_put_get_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        // Basic put and get
        db_map.put(b"key1", b"value1").unwrap();
        assert_eq!(db_map.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db_map.len().unwrap(), 1);

        // Empty key and value
        db_map.put(b"", b"").unwrap();
        assert_eq!(db_map.get(b"").unwrap(), Some(b"".to_vec()));
        assert_eq!(db_map.len().unwrap(), 2);

        // Non-UTF8 data
        let non_utf8_key = &[0xFF, 0xFE, 0xFD];
        let non_utf8_value = &[0x80, 0x81, 0x82];
        db_map.put(non_utf8_key, non_utf8_value).unwrap();
        assert_eq!(db_map.get(non_utf8_key).unwrap(), Some(non_utf8_value.to_vec()));
        assert_eq!(db_map.len().unwrap(), 3);

        // Overwrite
        db_map.put(b"key1", b"new_value1").unwrap();
        assert_eq!(db_map.get(b"key1").unwrap(), Some(b"new_value1".to_vec()));
        assert_eq!(db_map.len().unwrap(), 3);

        // Delete
        assert_eq!(db_map.delete(b"key1").unwrap(), Some(b"new_value1".to_vec()));
        assert_eq!(db_map.get(b"key1").unwrap(), None);
        assert_eq!(db_map.len().unwrap(), 2);
    }

    #[test]
    fn test_bulk_put_get_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_bulk_put_get_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k_non_utf8", &[0x80, 0x81]),
        ];
        db_map.bulk_put(&entries).unwrap();
        assert_eq!(db_map.len().unwrap(), 4);

        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3", b"k_non_utf8", b"k4"];
        let results = db_map.bulk_get(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some(b"v1".to_vec()),
                Some(b"v2".to_vec()),
                Some(b"v3".to_vec()),
                Some(vec![0x80, 0x81]),
                None
            ]
        );
    }

    #[test]
    fn test_bulk_delete_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_bulk_delete_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        db_map.put(b"k1", b"v1").unwrap();
        db_map.put(b"k2", b"v2").unwrap();
        db_map.put(b"k3", b"v3").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys_to_delete: Vec<&[u8]> = vec![b"k1", b"k3", b"k4"];
        let deleted_values = db_map.bulk_delete(&keys_to_delete).unwrap();
        assert_eq!(
            deleted_values,
            vec![
                Some(b"v1".to_vec()),
                Some(b"v3".to_vec()),
                None
            ]
        );
        assert_eq!(db_map.len().unwrap(), 1);
        assert_eq!(db_map.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db_map.get(b"k1").unwrap(), None);
        assert_eq!(db_map.get(b"k3").unwrap(), None);
    }

    #[test]
    fn test_includes_key_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_includes_key_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(!db_map.includes_key(b"key1").unwrap());
        db_map.put(b"key1", b"value1").unwrap();
        assert!(db_map.includes_key(b"key1").unwrap());
        assert!(!db_map.includes_key(b"key2").unwrap());

        let non_utf8_key = &[0xFF, 0xFE];
        db_map.put(non_utf8_key, b"non_utf8_val").unwrap();
        assert!(db_map.includes_key(non_utf8_key).unwrap());

        db_map.delete(b"key1").unwrap();
        assert!(!db_map.includes_key(b"key1").unwrap());
        assert!(db_map.includes_key(non_utf8_key).unwrap());
    }

    #[test]
    fn test_iterate_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_iterate_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let mut expected_keys = HashSet::new();
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            db_map.put(&key[..], &value[..]).unwrap();
            expected_keys.insert(key);
        }
        assert_eq!(db_map.len().unwrap(), 10);

        let mut observed_keys = HashSet::new();
        for (k, v) in &db_map {
            let key_vec = k.as_bytes().to_vec();
            let _value_vec = v.to_vec();
            // We can't assert on string representation for non-UTF8 keys/values
            // but we can check if they are present in our expected set.
            observed_keys.insert(key_vec);
        }
        assert_eq!(observed_keys.len(), 10);
        // Convert expected_keys to Vec<Vec<u8>> for comparison
        let expected_keys_vec: HashSet<Vec<u8>> = expected_keys.into_iter().collect();
        assert_eq!(observed_keys, expected_keys_vec);
    }

    #[test]
    fn test_keys_values_bytes() {
        let db_name = "target/tmp/test_dbbytes_extended/test_keys_values_bytes.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_bytes_with_params(
                "some_bytes_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries = vec![
            (b"alpha".to_vec(), b"1".to_vec()),
            (b"beta".to_vec(), b"2".to_vec()),
            (b"gamma".to_vec(), b"3".to_vec()),
        ];
        for (k, v) in &entries {
            db_map.put(&k[..], &v[..]).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), 3);

        let keys: HashSet<Vec<u8>> = db_map.keys().map(|k| k.as_bytes().to_vec()).collect();
        let expected_keys: HashSet<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, expected_keys);

        let values: HashSet<Vec<u8>> = db_map.values().map(|v| v.to_vec()).collect();
        let expected_values: HashSet<Vec<u8>> = entries.iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(values, expected_values);
    }
}
