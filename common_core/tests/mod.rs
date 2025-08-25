

#[cfg(test)]
mod err_test {
    use std::error::Error;
    use common_core::err::*;
    use std::io;

    fn create_temp_error() -> CommonImplError {
        create_error(
            COMMON_ERROR_CATEGORY, API_CALL_ERROR, "dddd".to_string(), None)
    }


    fn create_raw_temp_io_error() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "ssssss")
    }

    #[test]
    fn test_error_create() -> Result<(), Box<dyn Error>> {
        let e = create_temp_error();
        
        assert_eq!(*e.category_id(), COMMON_ERROR_CATEGORY);
        assert_eq!(e.cause_msg(), "dddd");
        Ok(())
    }

    #[test]
    fn test_error_create_use_source() -> Result<(), Box<dyn Error>> {
        let e = create_temp_error();
        
        let other_e = create_error(COMMON_ERROR_CATEGORY, CRITICAL_ERROR
            , "other error".to_string(), Some(Box::new(e)));

        assert_eq!("other error", other_e.msg());
        Ok(())
    }

    #[test]
    fn test_error_create_use_raw_source() -> Result<(), Box<dyn Error>> {
        let e = create_raw_temp_io_error();
        
        let other_e = create_error(COMMON_ERROR_CATEGORY, CRITICAL_ERROR
            , "other error".to_string(), Some(Box::new(e)));

        assert_eq!("[msg:other error, src:ssssss]", other_e.cause_msg());
        Ok(())
    }
}


#[cfg(test)]
mod pool_tests {
    use std::error::Error;
    use common_core::collection::pool::get_thread_safe_pool;
    use common_core::collection::pool::ThreadSafePool;
    use common_core::collection::pool::PoolItem;
    #[test]
    pub fn test_pool_arc() -> Result<(), Box<dyn Error>> {
        use std::sync::Arc;

        let p :Arc<dyn ThreadSafePool<(), ()>> = get_thread_safe_pool(String::from("test"),Box::new(|_x : ()| {
            return Ok(())
        }),5);

        {

            let _: Result<Box<dyn PoolItem<()>>, Box<dyn Error>> = p.get_owned(());
        }
        
        assert_eq!(1, p.alloc_size());

        {
            let mut a = p.get_owned(())?;
            a.dispose();
        }

        assert_eq!(0, p.alloc_size());

        Ok(()) 
    }
}