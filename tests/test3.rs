mod test3 {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbXxx, DbXxxBase};
    //use abyssiniandb::{DbBytes, DbInt, DbString};
    //
    #[test]
    fn test_insert_conflicts() {
        let db_name = "target/tmp/test_insert_conflicts.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_int_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        //
        db_map.put(&1, &[2_u8]).unwrap();
        db_map.put(&5, &[3_u8]).unwrap();
        db_map.put(&9, &[4_u8]).unwrap();
        //
        assert_eq!(db_map.get(&9).unwrap(), Some(vec![4_u8]));
        assert_eq!(db_map.get(&5).unwrap(), Some(vec![3_u8]));
        assert_eq!(db_map.get(&1).unwrap(), Some(vec![2_u8]));
    }
    #[test]
    fn test_delete_conflicts() {
        let db_name = "target/tmp/test_delete_conflicts.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_int_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        //
        db_map.put(&1, &[2_u8]).unwrap();
        assert_eq!(db_map.get(&1).unwrap(), Some(vec![2_u8]));
        //
        db_map.put(&5, &[3_u8]).unwrap();
        assert_eq!(db_map.get(&1).unwrap(), Some(vec![2_u8]));
        assert_eq!(db_map.get(&5).unwrap(), Some(vec![3_u8]));
        //
        db_map.put(&9, &[4_u8]).unwrap();
        assert_eq!(db_map.get(&1).unwrap(), Some(vec![2_u8]));
        assert_eq!(db_map.get(&5).unwrap(), Some(vec![3_u8]));
        assert_eq!(db_map.get(&9).unwrap(), Some(vec![4_u8]));
        //
        assert_eq!(db_map.delete(&1).unwrap(), Some(vec![2_u8]));
        assert_eq!(db_map.get(&9).unwrap(), Some(vec![4_u8]));
        assert_eq!(db_map.get(&5).unwrap(), Some(vec![3_u8]));
        assert_eq!(db_map.get(&1).unwrap(), None);
    }
    #[test]
    fn test_is_empty() {
        let db_name = "target/tmp/test_is_empty.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_int_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        //
        db_map.put(&1, &[2_u8]).unwrap();
        assert!(!db_map.is_empty().unwrap());
        assert_eq!(db_map.delete(&1).unwrap(), Some(vec![2_u8]));
        assert!(db_map.is_empty().unwrap());
    }
    #[cfg(feature = "large_test")]
    #[test]
    fn test_lots_of_insertions() {
        let db_name = "target/tmp/test_lots_of_insertions.abyssiniandb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let mut db_map = db
            .db_map_int_with_params(
                "some_u64_1",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(10000),
                    ..Default::default()
                },
            )
            .unwrap();
        //
        // Try this a few times to make sure we never screw up the hashmap's
        // internal state.
        for _ in 0..10 {
            assert!(db_map.is_empty().unwrap());
            //
            for i in 1..1001 {
                db_map.put(&i, &i.to_le_bytes()).unwrap();
                //
                for j in 1..=i {
                    let r = db_map.get(&j).unwrap();
                    assert_eq!(r, Some(j.to_le_bytes().to_vec()));
                }
                for j in i + 1..1001 {
                    let r = db_map.get(&j).unwrap();
                    assert_eq!(r, None);
                }
            }
            for i in 1001..2001 {
                assert!(!db_map.includes_key(&i).unwrap());
            }
            //
            // remove forwards
            for i in 1..1001 {
                assert!(db_map.delete(&i).unwrap().is_some());
                for j in 1..=i {
                    assert!(!db_map.includes_key(&j).unwrap());
                }
                for j in i + 1..1001 {
                    assert!(db_map.includes_key(&j).unwrap());
                }
            }
            for i in 1..1001 {
                assert!(!db_map.includes_key(&i).unwrap());
            }
            //
            for i in 1..1001 {
                db_map.put(&i, &i.to_le_bytes()).unwrap();
            }
            //
            // remove backwards
            for i in (1..1001).rev() {
                assert!(db_map.delete(&i).unwrap().is_some());
                for j in i..1001 {
                    assert!(!db_map.includes_key(&j).unwrap());
                }
                for j in 1..i {
                    //assert!(db_map.includes_key(&j).unwrap());
                    if !db_map.includes_key(&j).unwrap() {
                        panic!("{}:{}", i, j);
                    }
                }
            }
        }
    }
}
