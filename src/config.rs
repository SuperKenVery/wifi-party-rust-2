use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};

type AudioSample = u8;

pub const SAMPLE_RATE: i32 = 44100;
pub const QUEUE_SIZE: usize = 256;
pub const PORT: u16 = 3487;
pub const MCAST_ADDR: &str = "225.23.134.210";
pub const RECV_BUF_SIZE: usize = (SAMPLE_RATE as usize) * size_of::<AudioSample>(); // 1 sec of audio

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
