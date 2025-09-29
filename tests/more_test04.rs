//
// Tests for bulk string operations with DbI64 and DbU64 keys
//
mod test_bulk_string_operations {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbXxx, DbXxxBase};

    #[test]
    fn test_bulk_put_get_string_i64() {
        let db_name = "target/tmp/test_bulk_string_operations/test_bulk_put_get_string_i64.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_i64_with_params(
                "some_i64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries = vec![
            (&1_i64, "value1".to_string()),
            (&2_i64, "value2".to_string()),
            (&3_i64, "value3".to_string()),
        ];
        db_map.bulk_put_string(&entries).unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys = vec![&1_i64, &2_i64, &3_i64, &4_i64];
        let results = db_map.bulk_get_string(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some("value1".to_string()),
                Some("value2".to_string()),
                Some("value3".to_string()),
                None
            ]
        );
    }

    #[test]
    fn test_bulk_delete_string_i64() {
        let db_name = "target/tmp/test_bulk_string_operations/test_bulk_delete_string_i64.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_i64_with_params(
                "some_i64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        db_map.put_string(&1_i64, "value1").unwrap();
        db_map.put_string(&2_i64, "value2").unwrap();
        db_map.put_string(&3_i64, "value3").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys_to_delete = vec![&1_i64, &3_i64, &4_i64];
        let deleted_values = db_map.bulk_delete_string(&keys_to_delete).unwrap();
        assert_eq!(
            deleted_values,
            vec![
                Some("value1".to_string()),
                Some("value3".to_string()),
                None
            ]
        );
        assert_eq!(db_map.len().unwrap(), 1);
        assert_eq!(db_map.get_string(&2_i64).unwrap(), Some("value2".to_string()));
        assert_eq!(db_map.get_string(&1_i64).unwrap(), None);
        assert_eq!(db_map.get_string(&3_i64).unwrap(), None);
    }

    #[test]
    fn test_bulk_put_get_string_u64() {
        let db_name = "target/tmp/test_bulk_string_operations/test_bulk_put_get_string_u64.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_u64_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        let entries = vec![
            (&1_u64, "value1".to_string()),
            (&2_u64, "value2".to_string()),
            (&3_u64, "value3".to_string()),
        ];
        db_map.bulk_put_string(&entries).unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys = vec![&1_u64, &2_u64, &3_u64, &4_u64];
        let results = db_map.bulk_get_string(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some("value1".to_string()),
                Some("value2".to_string()),
                Some("value3".to_string()),
                None
            ]
        );
    }

    #[test]
    fn test_bulk_delete_string_u64() {
        let db_name = "target/tmp/test_bulk_string_operations/test_bulk_delete_string_u64.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_u64_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();

        db_map.put_string(&1_u64, "value1").unwrap();
        db_map.put_string(&2_u64, "value2").unwrap();
        db_map.put_string(&3_u64, "value3").unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys_to_delete = vec![&1_u64, &3_u64, &4_u64];
        let deleted_values = db_map.bulk_delete_string(&keys_to_delete).unwrap();
        assert_eq!(
            deleted_values,
            vec![
                Some("value1".to_string()),
                Some("value3".to_string()),
                None
            ]
        );
        assert_eq!(db_map.len().unwrap(), 1);
        assert_eq!(db_map.get_string(&2_u64).unwrap(), Some("value2".to_string()));
        assert_eq!(db_map.get_string(&1_u64).unwrap(), None);
        assert_eq!(db_map.get_string(&3_u64).unwrap(), None);
    }
}
