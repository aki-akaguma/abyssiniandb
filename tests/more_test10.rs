//
// Tests for the read_fill_buffer method
//
mod test_read_fill_buffer {
    use abyssiniandb::filedb::{FileDbParams, HashBucketsParam};
    use abyssiniandb::{DbXxx, DbXxxBase};
    use std::fs;

    fn setup_db_and_map(db_name: &str) -> (abyssiniandb::filedb::FileDb, abyssiniandb::filedb::FileDbMapDbString) {
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
    fn test_read_fill_buffer_after_write() {
        let db_name = "target/tmp/test_read_fill_buffer/test_read_fill_buffer_after_write.abyssiniandb";
        let _ = fs::remove_dir_all(db_name);
        let (db, mut db_map) = setup_db_and_map(db_name);

        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        // Close with explicit flush
        db_map.flush().unwrap();
        drop(db_map);
        drop(db);

        // Reopen and call read_fill_buffer
        let (_db_reopen, mut db_map_reopened) = setup_db_and_map(db_name);
        db_map_reopened.read_fill_buffer().unwrap();

        // Verify data can be read
        assert_eq!(db_map_reopened.len().unwrap(), 2);
        assert_eq!(db_map_reopened.get_string("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(db_map_reopened.get_string("key2").unwrap(), Some("value2".to_string()));
    }

    #[test]
    fn test_read_fill_buffer_after_modifications() {
        let db_name = "target/tmp/test_read_fill_buffer/test_read_fill_buffer_after_modifications.abyssiniandb";
        let _ = fs::remove_dir_all(db_name);
        let (db, mut db_map) = setup_db_and_map(db_name);

        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        db_map.put_string("key3", "value3").unwrap();

        db_map.put_string("key2", "new_value2").unwrap(); // Modify
        db_map.delete("key3").unwrap(); // Delete

        // Close without explicit flush/sync
        db_map.flush().unwrap();
        drop(db_map);
        drop(db);

        // Reopen and call read_fill_buffer
        let (_db_reopen, mut db_map_reopened) = setup_db_and_map(db_name);
        db_map_reopened.read_fill_buffer().unwrap();

        // Verify state
        assert_eq!(db_map_reopened.len().unwrap(), 2);
        assert_eq!(db_map_reopened.get_string("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(db_map_reopened.get_string("key2").unwrap(), Some("new_value2".to_string()));
        assert_eq!(db_map_reopened.get_string("key3").unwrap(), None);
    }

    #[test]
    fn test_read_fill_buffer_empty_db() {
        let db_name = "target/tmp/test_read_fill_buffer/test_read_fill_buffer_empty_db.abyssiniandb";
        let _ = fs::remove_dir_all(db_name);
        let (db, mut db_map) = setup_db_and_map(db_name);

        assert_eq!(db_map.len().unwrap(), 0);

        // Close with explicit flush
        db_map.flush().unwrap();
        drop(db_map);
        drop(db);

        // Reopen and call read_fill_buffer
        let (_db_reopen, mut db_map_reopened) = setup_db_and_map(db_name);
        db_map_reopened.read_fill_buffer().unwrap();

        // Verify state
        assert_eq!(db_map_reopened.len().unwrap(), 0);
    }
}
