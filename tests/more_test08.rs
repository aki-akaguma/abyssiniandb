
//
// Extended tests for DbU64 functionality
//
mod test_dbu64_extended {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbMap, DbXxx, DbXxxBase};
    use std::collections::HashSet;

    #[test]
    fn test_put_get_u64_edge_cases() {
        let db_name = "target/tmp/test_dbu64_extended/test_put_get_u64_edge_cases.abyssiniandb";
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

        // Test with 0
        db_map.put(&0, &[10_u8]).unwrap();
        assert_eq!(db_map.get(&0).unwrap(), Some(vec![10_u8]));
        assert_eq!(db_map.len().unwrap(), 1);

        // Test with u64::MAX
        db_map.put(&u64::MAX, &[20_u8]).unwrap();
        assert_eq!(db_map.get(&u64::MAX).unwrap(), Some(vec![20_u8]));
        assert_eq!(db_map.len().unwrap(), 2);

        // Overwrite 0
        db_map.put(&0, &[11_u8]).unwrap();
        assert_eq!(db_map.get(&0).unwrap(), Some(vec![11_u8]));
        assert_eq!(db_map.len().unwrap(), 2);

        // Delete u64::MAX
        assert_eq!(db_map.delete(&u64::MAX).unwrap(), Some(vec![20_u8]));
        assert_eq!(db_map.get(&u64::MAX).unwrap(), None);
        assert_eq!(db_map.len().unwrap(), 1);
    }

    #[test]
    fn test_bulk_put_get_u64() {
        let db_name = "target/tmp/test_dbu64_extended/test_bulk_put_get_u64.abyssiniandb";
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

        let entries = [
            (1_u64, vec![1_u8]),
            (100_u64, vec![100_u8]),
            (u64::MAX / 2, vec![128_u8]),
        ];
        let bulk_entries: Vec<(&u64, &[u8])> =
            entries.iter().map(|(k, v)| (k, v.as_slice())).collect();
        db_map.bulk_put(&bulk_entries).unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys = vec![&1_u64, &100_u64, &(u64::MAX / 2), &u64::MAX];
        let results = db_map.bulk_get(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some(vec![1_u8]),
                Some(vec![100_u8]),
                Some(vec![128_u8]),
                None
            ]
        );
    }

    #[test]
    fn test_bulk_delete_u64() {
        let db_name = "target/tmp/test_dbu64_extended/test_bulk_delete_u64.abyssiniandb";
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

        db_map.put(&10, &[10_u8]).unwrap();
        db_map.put(&20, &[20_u8]).unwrap();
        db_map.put(&30, &[30_u8]).unwrap();
        assert_eq!(db_map.len().unwrap(), 3);

        let keys_to_delete = vec![&10_u64, &30_u64, &40_u64];
        let deleted_values = db_map.bulk_delete(&keys_to_delete).unwrap();
        assert_eq!(
            deleted_values,
            vec![Some(vec![10_u8]), Some(vec![30_u8]), None]
        );
        assert_eq!(db_map.len().unwrap(), 1);
        assert_eq!(db_map.get(&20).unwrap(), Some(vec![20_u8]));
        assert_eq!(db_map.get(&10).unwrap(), None);
        assert_eq!(db_map.get(&30).unwrap(), None);
    }

    #[test]
    fn test_includes_key_u64() {
        let db_name = "target/tmp/test_dbu64_extended/test_includes_key_u64.abyssiniandb";
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

        assert!(!db_map.includes_key(&1_u64).unwrap());
        db_map.put(&1_u64, &[1_u8]).unwrap();
        assert!(db_map.includes_key(&1_u64).unwrap());
        assert!(!db_map.includes_key(&2_u64).unwrap());

        db_map.delete(&1_u64).unwrap();
        assert!(!db_map.includes_key(&1_u64).unwrap());
    }

    #[test]
    fn test_iterate_u64() {
        let db_name = "target/tmp/test_dbu64_extended/test_iterate_u64.abyssiniandb";
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

        let mut expected_keys = HashSet::new();
        for i in 0..10 {
            let key = i as u64;
            let value = vec![i as u8];
            db_map.put(&key, &value).unwrap();
            expected_keys.insert(key.into());
        }
        assert_eq!(db_map.len().unwrap(), 10);

        let mut observed_keys = HashSet::new();
        for (k, v) in &db_map {
            assert!(v.len() == 1);
            observed_keys.insert(k);
        }
        assert_eq!(observed_keys.len(), 10);
        assert_eq!(observed_keys, expected_keys);
    }

    #[test]
    fn test_keys_values_u64() {
        let db_name = "target/tmp/test_dbu64_extended/test_keys_values_u64.abyssiniandb";
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

        let entries = vec![(1_u64, vec![1_u8]), (2_u64, vec![2_u8]), (3_u64, vec![3_u8])];
        for (k, v) in &entries {
            db_map.put(k, v).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), 3);

        let keys: HashSet<abyssiniandb::DbU64> = db_map.keys().collect();
        let expected_keys: HashSet<abyssiniandb::DbU64> = entries.iter().map(|(k, _)| (*k).into()).collect();
        assert_eq!(keys, expected_keys);

        let values: HashSet<Vec<u8>> = db_map.values().collect();
        let expected_values: HashSet<Vec<u8>> = entries.iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(values, expected_values);
    }
}
