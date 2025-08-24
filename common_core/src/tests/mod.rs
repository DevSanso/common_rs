

#[cfg(test)]
mod err_test {
    use std::error::Error;
    use crate::err::*;
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
