//
// Tests for DbMapKeyType trait implementations
//
mod test_dbmap_key_type_implementations {
    use abyssiniandb::{DbBytes, DbI64, DbMapKeyType, DbString, DbU64, DbVu64};
    use std::cmp::Ordering;

    #[test]
    fn test_db_i64_key_type() {
        // Test signature
        assert_eq!(DbI64::signature(), *b"i64_le\0\0");

        // Test from_bytes and as_bytes
        let original_i64: i64 = 1234567890;
        let db_i64 = DbI64::from(&original_i64);
        assert_eq!(db_i64.as_bytes(), original_i64.to_le_bytes().as_slice());
        let converted_db_i64 = DbI64::from_bytes(original_i64.to_le_bytes().as_slice());
        assert_eq!(converted_db_i64, db_i64);

        // Test cmp_u8
        let key1 = DbI64::from(&100_i64);
        let key2 = DbI64::from(&200_i64);
        assert_eq!(key1.cmp_u8(&100_i64.to_le_bytes()), Ordering::Equal);
        assert_eq!(key1.cmp_u8(&200_i64.to_le_bytes()), Ordering::Less);
        assert_eq!(key2.cmp_u8(&100_i64.to_le_bytes()), Ordering::Greater);
    }

    #[test]
    fn test_db_u64_key_type() {
        // Test signature
        assert_eq!(DbU64::signature(), *b"u64_le\0\0");

        // Test from_bytes and as_bytes
        let original_u64: u64 = 9876543210;
        let db_u64 = DbU64::from(&original_u64);
        assert_eq!(db_u64.as_bytes(), original_u64.to_le_bytes().as_slice());
        let converted_db_u64 = DbU64::from_bytes(original_u64.to_le_bytes().as_slice());
        assert_eq!(converted_db_u64, db_u64);

        // Test cmp_u8
        let key1 = DbU64::from(&100_u64);
        let key2 = DbU64::from(&200_u64);
        assert_eq!(key1.cmp_u8(&100_u64.to_le_bytes()), Ordering::Equal);
        assert_eq!(key1.cmp_u8(&200_u64.to_le_bytes()), Ordering::Less);
        assert_eq!(key2.cmp_u8(&100_u64.to_le_bytes()), Ordering::Greater);
    }

    #[test]
    fn test_db_string_key_type() {
        // Test signature
        assert_eq!(DbString::signature(), *b"string\0\0");

        // Test from_bytes and as_bytes
        let original_string = "hello world";
        let db_string = DbString::from(original_string);
        assert_eq!(db_string.as_bytes(), original_string.as_bytes());
        let converted_db_string = DbString::from_bytes(original_string.as_bytes());
        assert_eq!(converted_db_string, db_string);

        // Test cmp_u8
        let key1 = DbString::from("apple");
        let key2 = DbString::from("banana");
        assert_eq!(key1.cmp_u8("apple".as_bytes()), Ordering::Equal);
        assert_eq!(key1.cmp_u8("banana".as_bytes()), Ordering::Less);
        assert_eq!(key2.cmp_u8("apple".as_bytes()), Ordering::Greater);
    }

    #[test]
    fn test_db_bytes_key_type() {
        // Test signature
        assert_eq!(DbBytes::signature(), *b"bytes\0\0\0");

        // Test from_bytes and as_bytes
        let original_bytes = vec![1, 2, 3, 4, 5];
        let db_bytes = DbBytes::from(original_bytes.as_slice());
        assert_eq!(db_bytes.as_bytes(), original_bytes.as_slice());
        let converted_db_bytes = DbBytes::from_bytes(original_bytes.as_slice());
        assert_eq!(converted_db_bytes, db_bytes);

        // Test cmp_u8
        let key1 = DbBytes::from(vec![10, 20].as_slice());
        let key2 = DbBytes::from(vec![30, 40].as_slice());
        assert_eq!(key1.cmp_u8(vec![10, 20].as_slice()), Ordering::Equal);
        assert_eq!(key1.cmp_u8(vec![30, 40].as_slice()), Ordering::Less);
        assert_eq!(key2.cmp_u8(vec![10, 20].as_slice()), Ordering::Greater);

        // Test with non-UTF8 bytes
        let non_utf8_bytes = vec![0xFF, 0xFE, 0xFD];
        let db_non_utf8 = DbBytes::from(non_utf8_bytes.as_slice());
        assert_eq!(db_non_utf8.as_bytes(), non_utf8_bytes.as_slice());
        let converted_db_non_utf8 = DbBytes::from_bytes(non_utf8_bytes.as_slice());
        assert_eq!(converted_db_non_utf8, db_non_utf8);
    }

    #[test]
    fn test_db_vu64_key_type() {
        // Test signature
        assert_eq!(DbVu64::signature(), *b"u64_le\0\0");

        // Test from_bytes and as_bytes
        let original_vu64: u64 = 12345678901234567890;
        let db_vu64 = DbVu64::from(&original_vu64);
        // DbVu64 uses variable length encoding, so as_bytes() won't be a fixed size
        let encoded_bytes = db_vu64.as_bytes().to_vec();
        let converted_db_vu64 = DbVu64::from_bytes(&encoded_bytes);
        assert_eq!(converted_db_vu64, db_vu64);

        // Test cmp_u8
        let key1 = DbVu64::from(&100_u64);
        let key2 = DbVu64::from(&200_u64);
        assert_eq!(key1.cmp_u8(DbVu64::from(&100_u64).as_bytes()), Ordering::Equal);
        assert_eq!(key1.cmp_u8(DbVu64::from(&200_u64).as_bytes()), Ordering::Less);
        assert_eq!(key2.cmp_u8(DbVu64::from(&100_u64).as_bytes()), Ordering::Greater);
    }
}
