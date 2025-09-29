//
// Tests for persistence features (flush, sync_all, sync_data)
//
mod test_persistence_features {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbXxx, DbXxxBase};
    use std::fs;

    fn setup_db(db_name: &str) -> (abyssiniandb::filedb::FileDb, abyssiniandb::filedb::FileDbMapDbString) {
        let _ = fs::remove_dir_all(db_name);
        let db = abyssiniandb::open_file(db_name).unwrap();
        let db_map = db
            .db_map_string_with_params(
                "test_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        (db, db_map)
    }

    fn reopen_db(db_name: &str) -> (abyssiniandb::filedb::FileDb, abyssiniandb::filedb::FileDbMapDbString) {
        let db = abyssiniandb::open_file(db_name).unwrap();
        let db_map = db
            .db_map_string_with_params(
                "test_map",
                FileDbParams {
                    buckets_size: HashBucketsParam::Capacity(4),
                    ..Default::default()
                },
            )
            .unwrap();
        (db, db_map)
    }

    #[test]
    fn test_flush_persists_data() {
        let db_name = "target/tmp/test_persistence_features/test_flush_persists_data.abyssiniandb";
        let (db, mut db_map) = setup_db(db_name);

        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        assert_eq!(db_map.len().unwrap(), 2);

        db_map.flush().unwrap();
        drop(db_map);
        drop(db);

        let (_db, mut db_map_reopened) = reopen_db(db_name);
        assert_eq!(db_map_reopened.len().unwrap(), 2);
        assert_eq!(db_map_reopened.get_string("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(db_map_reopened.get_string("key2").unwrap(), Some("value2".to_string()));
    }

    #[test]
    fn test_sync_all_persists_data() {
        let db_name = "target/tmp/test_persistence_features/test_sync_all_persists_data.abyssiniandb";
        let (db, mut db_map) = setup_db(db_name);

        db_map.put_string("keyA", "valueA").unwrap();
        db_map.put_string("keyB", "valueB").unwrap();
        assert_eq!(db_map.len().unwrap(), 2);

        db_map.sync_all().unwrap();
        drop(db_map);
        drop(db);

        let (_db, mut db_map_reopened) = reopen_db(db_name);
        assert_eq!(db_map_reopened.len().unwrap(), 2);
        assert_eq!(db_map_reopened.get_string("keyA").unwrap(), Some("valueA".to_string()));
        assert_eq!(db_map_reopened.get_string("keyB").unwrap(), Some("valueB".to_string()));
    }

    #[test]
    fn test_sync_data_persists_data() {
        let db_name = "target/tmp/test_persistence_features/test_sync_data_persists_data.abyssiniandb";
        let (db, mut db_map) = setup_db(db_name);

        db_map.put_string("keyX", "valueX").unwrap();
        db_map.put_string("keyY", "valueY").unwrap();
        assert_eq!(db_map.len().unwrap(), 2);

        db_map.sync_data().unwrap();
        drop(db_map);
        drop(db);

        let (_db, mut db_map_reopened) = reopen_db(db_name);
        assert_eq!(db_map_reopened.len().unwrap(), 2);
        assert_eq!(db_map_reopened.get_string("keyX").unwrap(), Some("valueX".to_string()));
        assert_eq!(db_map_reopened.get_string("keyY").unwrap(), Some("valueY".to_string()));
    }

}
