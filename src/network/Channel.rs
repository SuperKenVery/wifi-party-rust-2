use super::Packet::Packet;
use crate::config::{AudioBuffer, MCAST_ADDR, PORT, QUEUE_SIZE};
use anyhow::Error as AnyError;
use crossbeam::channel::{bounded, Receiver, Sender, TrySendError};
use log::{info, trace, warn};
use rkyv::rancor::Error;
use std::cell::RefCell;
use std::collections::{hash_map::HashMap, VecDeque};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};

struct Channel {
    // Channel properties
    id: i32,

    // Receive states
    rcv_buf_tx: Sender<AudioBuffer>,
    rcv_buf_rx: Receiver<AudioBuffer>,
    left_over: Mutex<Option<AudioBuffer>>,
    left_read_idx: Mutex<usize>, // Left-over buffer from last read, how much have been read in last read
    last_cont_idx: Mutex<i32>,   // Last continuous index
    last_recv_idx: Mutex<i32>,   // The max index we received
    next_read_idx: Mutex<i32>,   // Next index to read
    ofo_buf: Mutex<HashMap<i32, AudioBuffer>>, // Out-of-order buffer

    // Send states
    socket: Arc<UdpSocket>,
    seq: Mutex<i32>,
}

impl Channel {
    pub fn new(id: i32, socket: Arc<UdpSocket>) -> Channel {
        let (tx, rx) = bounded(QUEUE_SIZE);

        Channel {
            id,
            rcv_buf_rx: rx,
            rcv_buf_tx: tx,
            left_over: Mutex::new(None),
            left_read_idx: Mutex::new(0),
            last_cont_idx: Mutex::new(-1),
            last_recv_idx: Mutex::new(-1),
            next_read_idx: Mutex::new(0),
            ofo_buf: Mutex::new(HashMap::new()),
            socket,
            seq: Mutex::new(0),
        }
    }

    // === Audio receiving and playing ===

    // Send an audio buffer to channel
    // Receive (and drop) some buffers first if it's full
    fn force_enqueue(&self, audio: AudioBuffer) {
        let result = self.rcv_buf_tx.try_send(audio);

        if let Err(eres) = result {
            if let TrySendError::Disconnected(_) = eres {
                panic!("Channel {}'s receiver is disconnected", self.id);
            } else if let TrySendError::Full(audio) = eres {
                // Full, free up 1 space
                info!("Queue full, free up space by dropping 1 buffer");
                self.rcv_buf_rx.try_recv().unwrap();

                // Make sure the caller holds self.last_cont_idx and
                // self.last_recv_idx locks
                #[cfg(debug_assertions)]
                {
                    let res = self.last_cont_idx.try_lock();
                    assert!(matches!(res, Err(_)));
                    let res = self.last_recv_idx.try_lock();
                    assert!(matches!(res, Err(_)));
                }

                // By now, we surely have one space, because caller holds
                // self.last_cont_idx and self.last_recv_idx locks so no one can
                // send anything to this channel
                self.rcv_buf_tx.try_send(audio).unwrap();
            }
        }
    }

    // Receive audio from channel
    pub fn receive(&self, idx: i32, audio: AudioBuffer) {
        let mut last_cont_idx = self.last_cont_idx.lock().unwrap();
        let mut last_recv_idx = self.last_recv_idx.lock().unwrap();
        let next_read_idx = self.next_read_idx.lock().unwrap();

        if *last_cont_idx < *next_read_idx - 1 {
            // Last time when reading, we read all of continuous buffer
            // And read into the OFO buffer. So we need to advance
            // last_cont_idx so that we don't receive buffers we have played
            trace!(
                "last_cont_idx {}->{} because of reading",
                *last_cont_idx,
                *next_read_idx - 1
            );
            assert!(self.rcv_buf_rx.is_empty());
            *last_cont_idx = *next_read_idx - 1;
        }

        if idx <= *last_cont_idx {
            // Duplicate
            trace!(
                "Receive DUP #{} len={}, last_cont={}",
                idx,
                audio.buf.len(),
                *last_cont_idx
            );
            return;
        } else if idx == *last_cont_idx + 1 {
            // New content
            trace!("Receive CONT #{} len={}", idx, audio.buf.len());
            self.force_enqueue(audio);
            *last_cont_idx += 1;

            // Check OFO buffer
            let mut ofo_buf = self.ofo_buf.lock().unwrap();
            while ofo_buf.contains_key(&(*last_cont_idx + 1)) && *last_cont_idx <= *last_recv_idx {
                trace!(
                    "        CONT #{}: Connecting OFO #{} len={}",
                    idx,
                    *last_cont_idx + 1,
                    ofo_buf[&(*last_cont_idx + 1)].buf.len()
                );
                let audio = ofo_buf.remove(&(*last_cont_idx + 1)).unwrap();
                self.force_enqueue(audio);
                *last_cont_idx += 1;
            }
            return;
        } else {
            // Out of order
            trace!(
                "Receive OFO #{} len={}, last_cont_idx={}, last_recv_idx={}",
                idx,
                audio.buf.len(),
                *last_cont_idx,
                *last_recv_idx
            );
            let mut ofo_buf = self.ofo_buf.lock().unwrap();
            ofo_buf.insert(idx, audio);
            *last_recv_idx = idx.max(*last_recv_idx);
        }
    }

    pub fn read(&self, size: usize) -> AudioBuffer {
        let mut need_read = size;
        let mut buf = AudioBuffer::new_with_capacity(size as usize);
        trace!("Requested to read {} samples from channel", size);

        // Read from left-over buffer
        let mut next_read_idx = self.next_read_idx.lock().unwrap();
        let mut left_over = self.left_over.lock().unwrap();
        let mut left_read_idx = self.left_read_idx.lock().unwrap();
        if let Some(audio) = &*left_over {
            let audio_len = audio.buf.len() - *left_read_idx;
            if audio_len > need_read {
                trace!(
                    "Reading {}/{} samples from left-over buffer",
                    need_read,
                    audio_len
                );
                buf.buf
                    .extend_from_slice(&audio.buf[*left_read_idx..*left_read_idx + need_read]);
                *left_read_idx += need_read;
                need_read = 0;
            } else {
                trace!("Reading all {} samples from left-over buffer", audio_len);
                buf.buf.extend_from_slice(&audio.buf[*left_read_idx..]);
                need_read -= audio_len;
                *left_over = None;
                *next_read_idx += 1;
            }
        }

        if need_read == 0 {
            return buf;
        } // Enough, return, avoid lock

        let mut block_len = 1024;

        // Read from continuous buffer
        while need_read > 0 {
            let audio = self.rcv_buf_rx.try_recv();
            if let Ok(mut audio) = audio {
                if audio.buf.len() > need_read {
                    trace!(
                        "Read #{} len={} prev_total={} from continuous buffer, left over len={}",
                        *next_read_idx,
                        need_read,
                        buf.buf.len(),
                        audio.buf.len() - need_read
                    );
                    buf.buf.extend_from_slice(&audio.buf[0..need_read]);
                    *left_over = Some(audio);
                    *left_read_idx = need_read;
                    need_read = 0;
                    break;
                } else {
                    trace!(
                        "Read #{} len={} prev_total={} from continuous buffer",
                        *next_read_idx,
                        audio.buf.len(),
                        buf.buf.len()
                    );
                    need_read -= audio.buf.len();
                    block_len = audio.buf.len();
                    buf.buf.append(&mut audio.buf);
                }
                *next_read_idx += 1;
            } else {
                break;
            }
        }

        // Enough, return
        if need_read == 0 {
            return buf;
        }

        // Not enough, read from OFO buffer
        let mut ofo_buf = self.ofo_buf.lock().unwrap();
        let last_recv_idx = self.last_recv_idx.lock().unwrap();

        for i in *next_read_idx..=*last_recv_idx {
            if need_read == 0 {
                break;
            }

            if let Some(mut audio) = ofo_buf.remove(&i) {
                if audio.buf.len() > need_read {
                    trace!(
                        "Read #{} len={} prev_total={} from OFO buffer, left over len={}",
                        *next_read_idx,
                        need_read,
                        buf.buf.len(),
                        audio.buf.len() - need_read
                    );
                    buf.buf.extend_from_slice(&audio.buf[0..need_read]);
                    *left_over = Some(audio);
                    *left_read_idx = need_read;
                    need_read = 0;
                    break;
                } else {
                    trace!(
                        "Read #{} len={} prev_total={} from OFO buffer",
                        *next_read_idx,
                        audio.buf.len(),
                        buf.buf.len()
                    );
                    need_read -= audio.buf.len();
                    block_len = audio.buf.len();
                    buf.buf.append(&mut audio.buf);
                }
            } else {
                info!(
                    "#{} missing in OFO buf prev_total={}, pushing {} samples of silence",
                    *next_read_idx,
                    buf.buf.len(),
                    block_len.min(need_read)
                );
                if block_len > need_read {
                    buf.push_silence(need_read);
                    need_read = 0;
                    break;
                } else {
                    buf.push_silence(block_len);
                    need_read -= block_len;
                }
            }
            *next_read_idx += 1;
        }

        // Give up and fill with silence
        if need_read > 0 {
            info!(
                "Nothing to read, fill {} samples of silence, prev_total={}",
                need_read,
                buf.buf.len()
            );
            buf.push_silence(need_read);
        }

        return buf;
    }

    // === Audio sending ===

    // Send audio to channel
    fn send(&self, audio: AudioBuffer) -> Result<(), AnyError> {
        let mut seq = self.seq.lock().unwrap();

        let packet = Packet {
            channel_id: self.id,
            seq: *seq,
            audio,
        };

        *seq += 1;

        let bytes = rkyv::to_bytes::<Error>(&packet)?;
        self.socket.send(&bytes)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::Channel;
    use crate::config::{AudioBuffer, QUEUE_SIZE, SAMPLE_RATE};
    use crate::config::{MCAST_ADDR, PORT};
    use anyhow::Error as AnyError;
    use log::{info, LevelFilter};
    use rand::seq::SliceRandom;
    use rand::{self, Rng};
    use std::borrow::Borrow;
    use std::{
        net::{Ipv4Addr, UdpSocket},
        sync::Arc,
    };
    use swing::{ColorFormat, Config, Logger};

    fn setup() -> Result<(Arc<UdpSocket>, Channel), AnyError> {
        Logger::with_config(Config {
            color_format: Some(ColorFormat::Solid),
            level: LevelFilter::Trace,
            ..Default::default()
        })
        .init()?;

        let socket = UdpSocket::bind(&format!("0.0.0.0:{}", PORT))?;
        socket.join_multicast_v4(
            &MCAST_ADDR.parse::<Ipv4Addr>().unwrap(),
            &"0.0.0.0".parse::<Ipv4Addr>().unwrap(),
        )?;
        socket.connect(format!("{}:{}", MCAST_ADDR, PORT))?;
        let socket = Arc::new(socket);

        let channel = Channel::new(0, socket.clone());

        Ok((socket, channel))
    }

    fn gen_samples(test_len: usize) -> Vec<(i32, AudioBuffer)> {
        let mut left_len = test_len;
        let mut counter = 0;
        let mut idx = 0;
        let mut rng = rand::thread_rng();

        let mut result = Vec::new();

        while left_len > 0 {
            let min_len = 128;
            let abuf_len = rng.gen_range(min_len..=((SAMPLE_RATE as f32 * 0.1) as usize));
            // let abuf_len = 128;
            let abuf_len = abuf_len.min(left_len);
            let mut audio = AudioBuffer::new_with_capacity(abuf_len);

            for _ in 0..abuf_len {
                audio.buf.push(counter);
                counter = counter.wrapping_add(1);
            }
            left_len -= abuf_len;

            result.push((idx, audio));
            idx += 1;
        }

        assert!(result.len() <= QUEUE_SIZE, "We might have too many samples that Channel might drop some samples. Increase min_len or decrease test_len to fix this. As test is randomly generated, re-run may also fix this. ");

        return result;
    }

    fn check_read_cont(test_len: usize, read: AudioBuffer) -> Result<(), AnyError> {
        if read.buf.len() != test_len {
            return Err(anyhow::anyhow!("Wrong length read"));
        }

        let mut counter = 0;
        for i in 0..test_len {
            if read.buf[i] != counter {
                return Err(anyhow::anyhow!(
                    "Wrong value at {}: Expected {}, got {}",
                    i,
                    counter,
                    read.buf[i]
                ));
            }
            counter = counter.wrapping_add(1);
        }

        Ok(())
    }

    fn check_read_chunk(test_len: usize, channel: &Channel) -> Result<(), AnyError> {
        let mut counter = 0;
        let mut left_len = test_len;
        let mut rng = rand::thread_rng();

        while left_len > 0 {
            let target_len = rng.gen_range(1..=left_len);
            let read = channel.read(target_len);
            for i in 0..target_len {
                if read.buf[i] != counter {
                    return Err(anyhow::anyhow!(
                        "Wrong value at {}: Expected {}, got {}",
                        i,
                        counter,
                        read.buf[0]
                    ));
                }
                counter = counter.wrapping_add(1);
            }
            left_len -= target_len;
        }

        Ok(())
    }

    fn feed_audio_samples(channel: &Channel, samples: Vec<(i32, AudioBuffer)>) {
        for (idx, audio) in samples {
            channel.receive(idx, audio);
        }
    }

    #[test]
    fn single_thread_recv_continuous() -> Result<(), AnyError> {
        let (_socket, channel) = setup()?;
        let test_len = 114514;

        let samples = gen_samples(test_len);
        feed_audio_samples(&channel, samples);

        check_read_cont(test_len, channel.read(test_len))?;

        Ok(())
    }

    #[test]
    fn single_thread_recv_random() -> Result<(), AnyError> {
        let (_socket, mut channel) = setup()?;
        let test_len = 114514;

        let mut samples = gen_samples(test_len);

        let mut rng = rand::thread_rng();
        samples.shuffle(&mut rng);
        feed_audio_samples(&channel, samples);

        check_read_chunk(test_len, &mut channel)?;

        Ok(())
    }

    #[test]
    fn multi_thread_recv_random() -> Result<(), AnyError> {
        let (_socket, channel) = setup()?;
        let channel = Arc::new(channel);
        let test_len = 114514;
        let worker_num = 8;

        let mut samples = gen_samples(test_len);

        let mut rng = rand::thread_rng();
        samples.shuffle(&mut rng);

        let worker_samples = (0..worker_num)
            .map(|worker_idx| {
                let mut result = Vec::new();
                for i in (worker_idx..samples.len()).step_by(worker_num) {
                    let (idx, audio) = samples[i].clone();
                    result.push((idx, audio));
                }
                result
            })
            .collect::<Vec<_>>();

        for (i, samples) in worker_samples.iter().enumerate() {
            let idxs = samples.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();
            info!("Worker {}: {:?}", i, idxs)
        }

        let workers: Vec<_> = worker_samples
            .into_iter()
            .map(|samples| {
                let channel = channel.clone();
                std::thread::spawn(move || {
                    feed_audio_samples(&*channel, samples);
                })
            })
            .collect();

        for worker in workers {
            worker.join().unwrap();
        }

        check_read_chunk(test_len, &*channel)?;

        Ok(())
    }
}
