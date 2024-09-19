use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};

pub static SAMPLE_RATE: i32 = 44100;
pub static QUEUE_SIZE: usize = 256;
pub static PORT: u16 = 3487;
pub static MCAST_ADDR: &str = "225.23.134.210";

type AudioSample = u8;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct AudioBuffer {
    pub buf: Vec<AudioSample>,
}

impl AudioBuffer {
    pub fn new_with_capacity(capacity: usize) -> AudioBuffer {
        AudioBuffer {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn push_silence(&mut self, len: usize) {
        for _ in 0..len {
            self.buf.push(0);
        }
    }
}
