//
// New tests for DbString functionality
//
mod test_dbstring {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbMap, DbMapKeyType, DbXxx, DbXxxBase};
    use std::collections::HashSet;

    #[test]
    fn test_put_get_string() {
        let db_name = "target/tmp/test_dbstring/test_put_get_string.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(db_map.len().unwrap(), 0);
        db_map.put_string("key1", "value1").unwrap();
        assert_eq!(db_map.len().unwrap(), 1);
        db_map.put_string("key2", "value2").unwrap();
        assert_eq!(db_map.len().unwrap(), 2);

        assert_eq!(
            db_map.get_string("key1").unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(
            db_map.get_string("key2").unwrap(),
            Some("value2".to_string())
        );
        assert_eq!(db_map.get_string("key3").unwrap(), None);
    }

    #[test]
    fn test_bulk_put_get() {
        let db_name = "target/tmp/test_dbstring/test_bulk_put_get.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries = vec![
            ("k1", "v1".as_bytes()),
            ("k2", "v2".as_bytes()),
            ("k3", "v3".as_bytes()),
        ];
        db_map.bulk_put(&entries).unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys = vec!["k1", "k2", "k3", "k4"];
        let results = db_map.bulk_get(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some("v1".as_bytes().to_vec()),
                Some("v2".as_bytes().to_vec()),
                Some("v3".as_bytes().to_vec()),
                None
            ]
        );

        let string_keys = vec!["k1", "k2", "k3", "k4"];
        let string_results = db_map.bulk_get_string(&string_keys).unwrap();
        assert_eq!(
            string_results,
            vec![
                Some("v1".to_string()),
                Some("v2".to_string()),
                Some("v3".to_string()),
                None
            ]
        );
    }

    #[test]
    fn test_bulk_delete() {
        let db_name = "target/tmp/test_dbstring/test_bulk_delete.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        db_map.put_string("k1", "v1").unwrap();
        db_map.put_string("k2", "v2").unwrap();
        db_map.put_string("k3", "v3").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys_to_delete = vec!["k1", "k3", "k4"];
        let deleted_values = db_map.bulk_delete(&keys_to_delete).unwrap();
        assert_eq!(
            deleted_values,
            vec![
                Some("v1".as_bytes().to_vec()),
                Some("v3".as_bytes().to_vec()),
                None
            ]
        );
        assert_eq!(db_map.len().unwrap(), 1);
        assert_eq!(db_map.get_string("k2").unwrap(), Some("v2".to_string()));
        assert_eq!(db_map.get_string("k1").unwrap(), None);
        assert_eq!(db_map.get_string("k3").unwrap(), None);

        let string_keys_to_delete = vec!["k2"];
        let deleted_string_values = db_map.bulk_delete_string(&string_keys_to_delete).unwrap();
        assert_eq!(deleted_string_values, vec![Some("v2".to_string())]);
        assert_eq!(db_map.len().unwrap(), 0);
    }

    #[test]
    fn test_includes_key() {
        let db_name = "target/tmp/test_dbstring/test_includes_key.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(!db_map.includes_key("key1").unwrap());
        db_map.put_string("key1", "value1").unwrap();
        assert!(db_map.includes_key("key1").unwrap());
        assert!(!db_map.includes_key("key2").unwrap());

        db_map.delete("key1").unwrap();
        assert!(!db_map.includes_key("key1").unwrap());
    }

    #[test]
    fn test_iterate_string() {
        let db_name = "target/tmp/test_dbstring/test_iterate_string.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let mut expected_keys = HashSet::new();
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
            expected_keys.insert(key);
        }
        assert_eq!(db_map.len().unwrap(), 10);

        let mut observed_keys = HashSet::new();
        for (k, v) in &db_map {
            let key_str = String::from_utf8_lossy(k.as_bytes()).to_string();
            let value_str = String::from_utf8_lossy(&v).to_string();
            assert!(key_str.starts_with("key"));
            assert!(value_str.starts_with("value"));
            observed_keys.insert(key_str);
        }
        assert_eq!(observed_keys.len(), 10);
        assert_eq!(observed_keys, expected_keys);
    }

    #[test]
    fn test_keys_values_string() {
        let db_name = "target/tmp/test_dbstring/test_keys_values_string.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_string_with_params(
                "some_string_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries = vec![("alpha", "1"), ("beta", "2"), ("gamma", "3")];
        for (k, v) in &entries {
            db_map.put_string(*k, v).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), 3);

        let keys: HashSet<String> = db_map
            .keys()
            .map(|k| String::from_utf8_lossy(k.as_bytes()).to_string())
            .collect();
        let expected_keys: HashSet<String> = entries.iter().map(|(k, _)| k.to_string()).collect();
        assert_eq!(keys, expected_keys);

        let values: HashSet<String> = db_map
            .values()
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .collect();
        let expected_values: HashSet<String> = entries.iter().map(|(_, v)| v.to_string()).collect();
        assert_eq!(values, expected_values);
    }
}
