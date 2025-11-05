
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