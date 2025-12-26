
// quick implementation of a ring buffer

struct Ring<T>{
    head: u32,
    tail: u32,
    size: u32,
    mask: u32,
}
impl Ring{
    pub fn new(size:u32)->Self {
        Ring{0,0,size,size-1}
    }
}
