use super::channel::Channel;
use super::packet::Packet;
use super::ArchivedPacket;
use crate::config::RECV_BUF_SIZE;
use anyhow::anyhow;
use anyhow::Error as AnyError;
use log::{error, info, warn};
use rkyv;
use rkyv::rancor::Error as RkyvError;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::ops::Deref;
use std::sync::Arc;
use uninit::prelude::*;

// A Dispatcher listens at a socket, receives packets, and dispatches them to the appropriate channel.
// When invalid packets are encountered, they are ignored.
pub struct Dispatcher {
    channels: HashMap<i32, Arc<Channel>>,
    socket: Arc<UdpSocket>,
}

impl Dispatcher {
    fn new(channels: Vec<Arc<Channel>>, socket: Arc<UdpSocket>) -> Dispatcher {
        let mut channel_map = HashMap::new();
        for channel in channels.iter() {
            channel_map.insert(channel.id, Arc::clone(channel));
        }

        Dispatcher {
            channels: channel_map,
            socket,
        }
    }

    fn receive_one_packet(&self, buf: &mut [u8]) -> Result<(), AnyError> {
        // Receive packet from internet
        let len = self.socket.recv(buf)?;

        // Now we need to extract AudioBuffer and feed it to Channel
        //
        // Implementation 1 -- total zero-copy deserialization, 2 copies
        // 1) Clone the buf (so that we can move in step 3)
        // 2) Deserialize it with total zero-copy, get ArchivedPacket
        // 3) Move the ArchivedPacket (maybe with another wrapper type) to the channel
        // 4) Channel will do vec.extend_slice(&packet.audio.buf[..]) which is another copy
        //
        // Implementation 2 -- partial zero-copy deserialization, 2 copies
        // 1) Deserialize the buf with partial zero-copy
        // 2) Take the audio buffer from deserialized packet.
        //    Moving audio buffer from stack buffer would need a copy, either
        //    in step 1 or 2.
        // 3) Move the audio buffer to the channel, channel will do vec.extend_slice(&audio.buf[..])
        //    which is a copy
        //
        // Implementation 2 is simpler and requires less change, we'll go with that.
        //
        // But if we could receive into uninitialized buffer, implementation 1 should give
        // a 1-copy solution. We should switch to Implementation 1 when that's available.

        // Deserialize packet
        let archived = rkyv::access::<ArchivedPacket, RkyvError>(&buf[..len])?;
        let packet = rkyv::deserialize::<Packet, RkyvError>(archived).unwrap();

        // Dispatch packet to channel
        let channel = self
            .channels
            .get(&packet.channel_id)
            .ok_or(anyhow!("Invalid channel: {}", packet.channel_id))?;
        channel.receive(packet.seq, packet.audio);

        Ok(())
    }

    pub fn run(&self) {
        // TODO: When can we use heap-allocated MaybeUninit here? Initializing is as slow as copying...
        // So I'll currently use a stack-allocated buffer which gets initialized only once per program run.
        let mut buf = [0 as u8; RECV_BUF_SIZE];

        loop {
            match self.receive_one_packet(&mut buf) {
                Ok(_) => {}
                Err(e) => {
                    error!("Error receiving packet: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QUEUE_SIZE;
    use std::net::UdpSocket;
    use std::sync::Arc;
}
