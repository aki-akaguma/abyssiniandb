//
// Tests for multiple FileDbMap instances within a single FileDb
//
mod test_multiple_db_maps {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbXxx, DbXxxBase};
    use std::fs;

    fn setup_db_and_maps(db_name: &str) -> (abyssiniandb::filedb::FileDb, abyssiniandb::filedb::FileDbMapDbString, abyssiniandb::filedb::FileDbMapDbI64) {
        let _ = fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let string_map = db
            .db_map_string_with_params(
                "string_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        let i64_map = db
            .db_map_i64_with_params(
                "i64_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        (db, string_map, i64_map)
    }

    #[test]
    fn test_multiple_string_maps_isolation() {
        let db_name = "target/tmp/test_multiple_db_maps/test_multiple_string_maps_isolation.abyssiniandb";
        let _ = fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();

        let mut map1 = db
            .db_map_string_with_params(
                "map1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut map2 = db
            .db_map_string_with_params(
                "map2",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        map1.put_string("key1", "value1_map1").unwrap();
        map2.put_string("key1", "value1_map2").unwrap();

        assert_eq!(map1.get_string("key1").unwrap(), Some("value1_map1".to_string()));
        assert_eq!(map2.get_string("key1").unwrap(), Some("value1_map2".to_string()));
        assert_eq!(map1.len().unwrap(), 1);
        assert_eq!(map2.len().unwrap(), 1);

        map1.put_string("key2", "value2_map1").unwrap();
        assert_eq!(map1.get_string("key2").unwrap(), Some("value2_map1".to_string()));
        assert_eq!(map2.get_string("key2").unwrap(), None);
    }

    #[test]
    fn test_mixed_type_maps_isolation() {
        let db_name = "target/tmp/test_multiple_db_maps/test_mixed_type_maps_isolation.abyssiniandb";
        let (db, mut string_map, mut i64_map) = setup_db_and_maps(db_name);

        string_map.put_string("str_key", "str_value").unwrap();
        i64_map.put(&123_i64, b"i64_value").unwrap();

        assert_eq!(string_map.get_string("str_key").unwrap(), Some("str_value".to_string()));
        assert_eq!(i64_map.get(&123_i64).unwrap(), Some(b"i64_value".to_vec()));

        // Ensure no cross-talk
        assert_eq!(string_map.get_string("123").unwrap(), None);
        assert_eq!(i64_map.get(&0_i64).unwrap(), None);

        drop(string_map);
        drop(i64_map);
        db.sync_all().unwrap();
        drop(db);

        // Reopen and verify
        let db_reopen = abyssiniandb::open_file(db_name).unwrap();
        let mut string_map_reopen = db_reopen
            .db_map_string_with_params(
                "string_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut i64_map_reopen = db_reopen
            .db_map_i64_with_params(
                "i64_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(string_map_reopen.get_string("str_key").unwrap(), Some("str_value".to_string()));
        assert_eq!(i64_map_reopen.get(&123_i64).unwrap(), Some(b"i64_value".to_vec()));
    }

    #[test]
    fn test_reopening_multiple_maps() {
        let db_name = "target/tmp/test_multiple_db_maps/test_reopening_multiple_maps.abyssiniandb";
        let (db, mut map_str, mut map_i64) = setup_db_and_maps(db_name);

        map_str.put_string("k1", "v1").unwrap();
        map_i64.put(&1, b"val1").unwrap();

        assert_eq!(map_str.len().unwrap(), 1);
        assert_eq!(map_i64.len().unwrap(), 1);

        drop(map_str);
        drop(map_i64);
        db.sync_all().unwrap();
        drop(db);

        // Reopen the main db and then each map
        let db_reopen = abyssiniandb::open_file(db_name).unwrap();
        let mut map_str_reopen = db_reopen
            .db_map_string_with_params(
                "string_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut map_i64_reopen = db_reopen
            .db_map_i64_with_params(
                "i64_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(map_str_reopen.get_string("k1").unwrap(), Some("v1".to_string()));
        assert_eq!(map_i64_reopen.get(&1).unwrap(), Some(b"val1".to_vec()));
        assert_eq!(map_str_reopen.len().unwrap(), 1);
        assert_eq!(map_i64_reopen.len().unwrap(), 1);
    }

    #[test]
    fn test_map_creation_with_different_params() {
        let db_name = "target/tmp/test_multiple_db_maps/test_map_creation_with_different_params.abyssiniandb";
        let _ = fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();

        let mut map_small_buckets = db
            .db_map_string_with_params(
                "small_buckets_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(2),
                    ..Default::default()
                },
            )
            .unwrap();

        let mut map_large_buckets = db
            .db_map_string_with_params(
                "large_buckets_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(100),
                    ..Default::default()
                },
            )
            .unwrap();

        map_small_buckets.put_string("k1", "v1").unwrap();
        map_large_buckets.put_string("k1", "v1").unwrap();

        assert_eq!(map_small_buckets.len().unwrap(), 1);
        assert_eq!(map_large_buckets.len().unwrap(), 1);

        // Verify they are independent and functional
        assert_eq!(map_small_buckets.get_string("k1").unwrap(), Some("v1".to_string()));
        assert_eq!(map_large_buckets.get_string("k1").unwrap(), Some("v1".to_string()));
    }
}
