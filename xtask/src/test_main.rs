//use anyhow::Context;
//use std::io::BufRead;
use abyssiniandb::filedb::{
    CheckFileDbMap, FileBufSizeParam, FileDbMap, FileDbMapDbString, FileDbParams, HashBucketsParam,
};
use abyssiniandb::{DbMap, DbMapKeyType, DbString};
use abyssiniandb::{DbXxx, DbXxxBase};
use std::str::FromStr;

pub fn run(program: &str, args: &[&str]) -> anyhow::Result<()> {
    if args.len() < 1 {
        println!("[usage] {} {{ {} }}", program, "1|2|3|4|5");
        return Ok(());
    }
    match args[0] {
        "1" => test01(&args[1..])?,
        "2" => test02(&args[1..])?,
        "3" => test03(&args[1..])?,
        "4" => test04(&args[1..])?,
        "5" => test05(program, &args[1..])?,
        _ => (),
    }
    Ok(())
}

//use abyssiniandb::filedb::CheckFileDbMap;
//use abyssiniandb::filedb::FileDbMapDbInt;
//use abyssiniandb::filedb::FileDbMapDbString;
//use abyssiniandb::DbMap;
//use abyssiniandb::{DbXxx, DbXxxBase};

macro_rules! assert_get_eq {
    ($db_map:expr, $key:expr, $value:expr) => {
        assert_eq!($db_map.get_string($key)?, Some(($value).to_string()));
    };
}

macro_rules! assert_get_eq_none {
    ($db_map:expr, $key:expr) => {
        assert_eq!($db_map.get_string($key)?, None);
    };
}

fn test01(_args: &[&str]) -> anyhow::Result<()> {
    let db_name = "target/tmp/testA01.abyssiniandb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = abyssiniandb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    {
        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        db_map.put_string("key3", "value3").unwrap();
        db_map.put_string("key4", "value4").unwrap();
        db_map.put_string("key5", "value5").unwrap();
        //
        db_map.put_string("key6", "value6").unwrap();
        db_map.put_string("key7", "value7").unwrap();
        db_map.put_string("key8", "value8").unwrap();
        db_map.put_string("key9", "value9").unwrap();
        //
        db_map.sync_data().unwrap();
    }
    //
    /*
    _print_check_db_map(
        &db_map,
        CheckC {
            ..Default::default()
        },
    );
    */
    //
    //println!("{}", db_map.graph_string().unwrap());
    //
    {
        assert_get_eq!(db_map, "key1", "value1");
        assert_get_eq!(db_map, "key2", "value2");
        assert_get_eq!(db_map, "key3", "value3");
        assert_get_eq!(db_map, "key4", "value4");
        assert_get_eq!(db_map, "key5", "value5");
        //
        assert_get_eq!(db_map, "key6", "value6");
        assert_get_eq!(db_map, "key7", "value7");
        assert_get_eq!(db_map, "key8", "value8");
        assert_get_eq!(db_map, "key9", "value9");
    }
    {
        //db_map.delete("key1").unwrap();
        //db_map.delete("key2").unwrap();
        //db_map.delete("key3").unwrap();
        db_map.delete("key4").unwrap();
        //db_map.delete("key5").unwrap();
        //db_map.delete("key6").unwrap();
        //db_map.delete("key7").unwrap();
        //db_map.delete("key8").unwrap();
        //db_map.delete("key9").unwrap();
        //
        db_map.sync_data().unwrap();
    }
    {
        assert_get_eq_none!(db_map, "key4");
    }
    //
    //println!("{}", db_map.graph_string().unwrap());
    //
    /*
    _print_check_db_map(
        &db_map,
        CheckC {
            ..Default::default()
        },
    );
    */
    //
    Ok(())
}

fn test02(_args: &[&str]) -> anyhow::Result<()> {
    let db_name = "target/tmp/testA02.abyssiniandb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = abyssiniandb::open_file(db_name).unwrap();
    let db_map = db.db_map_string("some_map1").unwrap();
    //
    {
        assert_eq!(db_map.iter().next(), None);
    }
    //
    Ok(())
}

fn test03(_args: &[&str]) -> anyhow::Result<()> {
    //const MAX_VAL: u64 = 1001;
    const MAX_VAL: u64 = 11;
    //
    let db_name = "target/tmp/testA03.abyssiniandb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = abyssiniandb::open_file(db_name).unwrap();
    let mut db_map = db
        .db_map_int_with_params(
            "some_u64_1",
            FileDbParams {
                buckets_size: HashBucketsParam::Capacity(10),
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
        /*
        for i in 1..MAX_VAL {
            db_map.put(&i, &i.to_le_bytes()).unwrap();
            //
            for j in 1..=i {
                let r = db_map.get(&j).unwrap();
                assert_eq!(r, Some(j.to_le_bytes().to_vec()));
            }
            for j in i + 1..MAX_VAL {
                let r = db_map.get(&j).unwrap();
                assert_eq!(r, None);
            }
        }
        for i in MAX_VAL..(2*MAX_VAL) {
            assert!(!db_map.includes_key(&i).unwrap());
        }
        assert_eq!(db_map.len().unwrap(), MAX_VAL - 1);
        //
        // remove forwards
        for i in 1..MAX_VAL {
            assert!(db_map.delete(&i).unwrap().is_some());
            for j in 1..=i {
                assert!(!db_map.includes_key(&j).unwrap());
            }
            for j in i + 1..MAX_VAL {
                assert!(db_map.includes_key(&j).unwrap());
            }
        }
        assert!(db_map.is_empty().unwrap());
        for i in 1..MAX_VAL {
            assert!(!db_map.includes_key(&i).unwrap());
        }
        */
        //
        for i in 1..MAX_VAL {
            db_map.put(&i, &i.to_le_bytes()).unwrap();
        }
        assert_eq!(db_map.len().unwrap(), MAX_VAL - 1);
        //
        /*
        #[rustfmt::skip]
        _print_check_db_map(&db_map, CheckC { ..Default::default() });
        */
        //
        // remove backwards
        for i in (1..MAX_VAL).rev() {
            assert!(db_map.delete(&i).unwrap().is_some());
            //
            /*
            #[rustfmt::skip]
            _print_check_db_map(&db_map, CheckC { ..Default::default() });
            */
            //
            for j in i..MAX_VAL {
                assert!(!db_map.includes_key(&j).unwrap());
            }
            //
            for j in 1..i {
                //assert!(db_map.includes_key(&j).unwrap());
                if !db_map.includes_key(&j).unwrap() {
                    panic!("{}:{}", i, j);
                }
            }
        }
        assert!(db_map.is_empty().unwrap());
        //println!("");
    }
    Ok(())
}

fn test04(_args: &[&str]) -> anyhow::Result<()> {
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone, Copy)]
struct CheckC {
    f_graph: bool,
}

fn _print_check_db_map<KT>(db_map: &FileDbMap<KT>, _check_cnf: CheckC)
where
    KT: DbMapKeyType + std::fmt::Display,
    FileDbMap<KT>: CheckFileDbMap,
{
    /*
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    println!(
        "key piece free: {:?}",
        db_map.count_of_free_key_piece().unwrap()
    );
    let (key_rec_v, val_rec_v, node_v) = db_map.count_of_used_node().unwrap();
    println!("key piece used: {:?}", key_rec_v);
    println!(
        "value piece free: {:?}",
        db_map.count_of_free_value_piece().unwrap()
    );
    println!("value piece used: {:?}", val_rec_v);
    println!("node free: {:?}", db_map.count_of_free_node().unwrap());
    println!("node used: {:?}", node_v);
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    if check_cnf.f_mst {
        println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    }
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    #[cfg(feature = "buf_stats")]
    println!("db_map.buf_stats(): {:?}", db_map.buf_stats());
    */
    /*
    println!(
        "key_piece_size_stats(): {}",
        db_map.key_piece_size_stats().unwrap()
    );
    println!(
        "value_piece_size_stats(): {}",
        db_map.value_piece_size_stats().unwrap()
    );
    */
    println!("key_length_stats(): {}", db_map.key_length_stats().unwrap());
    /*
    println!(
        "value_length_stats(): {}",
        db_map.value_length_stats().unwrap()
    );
    */
}

fn test05(program: &str, args: &[&str]) -> anyhow::Result<()> {
    if args.len() < 1 {
        println!("[usage] {} 5 {{-c|-w|-r|-d}}", program);
        return Ok(());
    }
    let db_name = "target/tmp/testA05.abyssiniandb";
    match args[0] {
        "-c" => test05_create(db_name)?,
        "-w" => test05_write(db_name)?,
        "-r" => test05_read(db_name)?,
        "-d" => test05_delete(db_name)?,
        _ => {
            eprintln!("[usage] test_main 5 {{-c|-w|-r|-d}}");
        }
    }
    Ok(())
}

const LOOP_MAX: i64 = 2_000_000;
const BULK_COUNT: i64 = 10_000;

fn test05_open_db_map(db_name: &str) -> Result<FileDbMapDbString, std::io::Error> {
    let db = abyssiniandb::open_file(db_name).unwrap();
    db.db_map_string_with_params(
        "some_map1",
        FileDbParams {
            htx_buf_size: FileBufSizeParam::PerMille(1000),
            idx_buf_size: FileBufSizeParam::PerMille(1000),
            key_buf_size: FileBufSizeParam::PerMille(1000),
            val_buf_size: FileBufSizeParam::PerMille(1000),
            /*
            key_buf_size: FileBufSizeParam::PerMille(100),
            idx_buf_size: FileBufSizeParam::PerMille(300),
            key_buf_size: FileBufSizeParam::Auto,
            idx_buf_size: FileBufSizeParam::Auto,
            */
            ..Default::default()
        },
    )
}

fn test05_conv_to_kv_string(ki: i64, _vi: i64) -> (DbString, String) {
    let bytes = ki.to_le_bytes();
    //let k = format!("{}.{}", bytes[0], bytes[1]);
    //let k = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
    //let k = format!("key-{}.{}.{}", bytes[0], bytes[1], bytes[2]);
    let k = format!("key-{}.{}.{}", bytes[0], bytes[1], bytes[2]).repeat(2);
    let v = format!("value-{}", ki).repeat(4);
    //let v = format!("value-{}", ki);
    //let v = format!("{}", _vi);
    //let v = String::new();
    (k.into(), v)
}

fn test05_create(db_name: &str) -> Result<(), std::io::Error> {
    let _ = std::fs::remove_dir_all(db_name);
    test05_write(db_name)
}

fn test05_write(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = test05_open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = test05_conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            match i64::from_str(&s) {
                Ok(i) => i + 1,
                Err(_) => 0,
            }
        } else {
            0
        }
    };
    //
    let mut kv_vec: Vec<(DbString, String)> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
            _test05_write_one(&mut db_map, &kv_vec)?;
            kv_vec.clear();
        }
        let (k, v) = test05_conv_to_kv_string(ki, vi);
        kv_vec.push((k, v));
    }
    if !kv_vec.is_empty() {
        _test05_write_one(&mut db_map, &kv_vec)?;
    }
    db_map.flush()
}

fn _test05_write_one(
    db_map: &mut FileDbMapDbString,
    key_vec: &[(DbString, String)],
) -> Result<(), std::io::Error> {
    let keys: Vec<(&DbString, &[u8])> = key_vec.iter().map(|(a, b)| (a, b.as_bytes())).collect();
    db_map.bulk_put(&keys)
}

fn test05_read(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = test05_open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = test05_conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            i64::from_str(&s).unwrap_or(0)
        } else {
            0
        }
    };
    //
    let mut key_vec: Vec<DbString> = Vec::new();
    let mut value_vec: Vec<String> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
            _test05_read_one(&mut db_map, &key_vec, &value_vec)?;
            //
            key_vec.clear();
            value_vec.clear();
        }
        let (k, correct) = test05_conv_to_kv_string(ki, vi);
        key_vec.push(k);
        value_vec.push(correct);
    }
    if !key_vec.is_empty() {
        _test05_read_one(&mut db_map, &key_vec, &value_vec)?;
        //
        key_vec.clear();
        value_vec.clear();
    }
    Ok(())
}

fn _test05_read_one(
    db_map: &mut FileDbMapDbString,
    key_vec: &[DbString],
    value_vec: &[String],
) -> Result<(), std::io::Error> {
    let keys: Vec<&DbString> = key_vec.iter().collect();
    let result = db_map.bulk_get_string(&keys)?;
    //
    for (idx, answer) in result.iter().enumerate() {
        if let Some(answer) = answer {
            let correct = &value_vec[idx];
            if answer != correct {
                panic!("invalid value: {:?} != {:?}", answer, correct);
            }
        } else {
            panic!("not found value: {} => {}", key_vec[idx], value_vec[idx]);
        }
    }
    Ok(())
}

fn test05_delete(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = test05_open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = test05_conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            i64::from_str(&s).unwrap_or(0)
        } else {
            0
        }
    };
    //
    let mut key_vec: Vec<DbString> = Vec::new();
    let mut value_vec: Vec<String> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
            _test05_delete_one(&mut db_map, &key_vec, &value_vec)?;
            //
            key_vec.clear();
            value_vec.clear();
        }
        let (k, correct) = test05_conv_to_kv_string(ki, vi);
        key_vec.push(k);
        value_vec.push(correct);
    }
    if !key_vec.is_empty() {
        _test05_delete_one(&mut db_map, &key_vec, &value_vec)?;
        //
        key_vec.clear();
        value_vec.clear();
    }
    Ok(())
}

fn _test05_delete_one(
    db_map: &mut FileDbMapDbString,
    key_vec: &[DbString],
    value_vec: &[String],
) -> Result<(), std::io::Error> {
    let keys: Vec<&DbString> = key_vec.iter().collect();
    let result = db_map.bulk_delete_string(&keys)?;
    //
    for (idx, answer) in result.iter().enumerate() {
        if let Some(answer) = answer {
            let correct = &value_vec[idx];
            if answer != correct {
                panic!("invalid value: {:?} != {:?}", answer, correct);
            }
        } else {
            panic!("not found value: {} => {}", key_vec[idx], value_vec[idx]);
        }
    }
    Ok(())
}
