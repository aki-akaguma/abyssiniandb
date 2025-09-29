//
// Tests for comprehensive iteration features with DbI64 keys
//
mod test_iteration_features {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbMap, DbXxx, DbXxxBase};
    use std::collections::HashSet;

    #[test]
    fn test_iter_mut_i64() {
        let db_name = "target/tmp/test_iteration_features/test_iter_mut_i64.abyssiniandb";
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

        db_map.put(&1, &[10_u8]).unwrap();
        db_map.put(&2, &[20_u8]).unwrap();
        db_map.put(&3, &[30_u8]).unwrap();

        let mut updates = Vec::new();
        for (k, v) in db_map.iter() {
            updates.push((k, vec![v[0] + 1_u8]));
        }

        for (k, new_v) in updates {
            db_map.put(&k, &new_v).unwrap();
        }

        assert_eq!(db_map.get(&1).unwrap(), Some(vec![11_u8]));
        assert_eq!(db_map.get(&2).unwrap(), Some(vec![21_u8]));
        assert_eq!(db_map.get(&3).unwrap(), Some(vec![31_u8]));
    }

    #[test]
    fn test_keys_values_i64_after_modifications() {
        let db_name = "target/tmp/test_iteration_features/test_keys_values_i64_after_modifications.abyssiniandb";
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

        db_map.put(&1, &[10_u8]).unwrap();
        db_map.put(&2, &[20_u8]).unwrap();
        db_map.put(&3, &[30_u8]).unwrap();

        let keys: HashSet<i64> = db_map.keys().map(|k| k.into()).collect();
        assert_eq!(keys, HashSet::from([1_i64, 2_i64, 3_i64]));

        let values: HashSet<Vec<u8>> = db_map.values().collect();
        assert_eq!(values, HashSet::from([vec![10_u8], vec![20_u8], vec![30_u8]]));

        // Modify a value
        db_map.put(&2, &[25_u8]).unwrap();

        let keys_after_mod: HashSet<i64> = db_map.keys().map(|k| k.into()).collect();
        assert_eq!(keys_after_mod, HashSet::from([1_i64, 2_i64, 3_i64]));

        let values_after_mod: HashSet<Vec<u8>> = db_map.values().collect();
        assert_eq!(values_after_mod, HashSet::from([vec![10_u8], vec![25_u8], vec![30_u8]]));
    }

    #[test]
    fn test_iteration_empty_map_i64() {
        let db_name = "target/tmp/test_iteration_features/test_iteration_empty_map_i64.abyssiniandb";
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

        assert_eq!(db_map.iter().count(), 0);
        assert_eq!(db_map.iter_mut().count(), 0);
        assert_eq!(db_map.keys().count(), 0);
        assert_eq!(db_map.values().count(), 0);
    }

    #[test]
    fn test_iteration_after_deletions_i64() {
        let db_name = "target/tmp/test_iteration_features/test_iteration_after_deletions_i64.abyssiniandb";
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

        db_map.put(&1, &[10_u8]).unwrap();
        db_map.put(&2, &[20_u8]).unwrap();
        db_map.put(&3, &[30_u8]).unwrap();
        db_map.put(&4, &[40_u8]).unwrap();

        assert_eq!(db_map.len().unwrap(), 4);

        db_map.delete(&2).unwrap();
        db_map.delete(&4).unwrap();

        assert_eq!(db_map.len().unwrap(), 2);

        let keys: HashSet<i64> = db_map.keys().map(|k| k.into()).collect();
        assert_eq!(keys, HashSet::from([1_i64, 3_i64]));

        let values: HashSet<Vec<u8>> = db_map.values().collect();
        assert_eq!(values, HashSet::from([vec![10_u8], vec![30_u8]]));

        let iterated_pairs: HashSet<(i64, Vec<u8>)> = db_map.iter().map(|(k, v)| (k.into(), v)).collect();
        assert_eq!(iterated_pairs, HashSet::from([(1_i64, vec![10_u8]), (3_i64, vec![30_u8])]));
    }
}
