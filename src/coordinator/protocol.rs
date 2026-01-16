use std::io;
use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Wire protocol message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Fence data from a rank
    FenceData = 1,
    /// Fence completion with all collected data
    FenceComplete = 2,
    /// Request for modex data
    ModexRequest = 3,
    /// Response with modex data
    ModexResponse = 4,
    /// Acknowledgment
    Ack = 5,
    /// Error response
    ErrorResponse = 6,
}

impl TryFrom<u8> for MessageType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match value {
            1 => Ok(MessageType::FenceData),
            2 => Ok(MessageType::FenceComplete),
            3 => Ok(MessageType::ModexRequest),
            4 => Ok(MessageType::ModexResponse),
            5 => Ok(MessageType::Ack),
            6 => Ok(MessageType::ErrorResponse),
            _ => Err(ProtocolError::InvalidMessageType(value)),
        }
    }
}

/// Coordination protocol message
#[derive(Debug, Clone)]
pub enum CoordMessage {
    /// Fence data: rank's data blob to be shared
    FenceData {
        fence_id: u64,
        rank: u32,
        data: Bytes,
    },
    /// Fence completion: collected data from all ranks
    FenceComplete {
        fence_id: u64,
        /// Vec of (rank, data) pairs
        all_data: Vec<(u32, Bytes)>,
    },
    /// Request modex data for a specific rank
    ModexRequest {
        request_id: u64,
        nspace: String,
        rank: u32,
    },
    /// Response with modex data
    ModexResponse {
        request_id: u64,
        data: Option<Bytes>,
    },
    /// Simple acknowledgment
    Ack { request_id: u64 },
    /// Error response
    Error { request_id: u64, message: String },
}

impl CoordMessage {
    /// Serialize message to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        match self {
            CoordMessage::FenceData {
                fence_id,
                rank,
                data,
            } => {
                buf.put_u8(MessageType::FenceData as u8);
                buf.put_u64(*fence_id);
                buf.put_u32(*rank);
                buf.put_u32(data.len() as u32);
                buf.put_slice(data);
            }
            CoordMessage::FenceComplete { fence_id, all_data } => {
                buf.put_u8(MessageType::FenceComplete as u8);
                buf.put_u64(*fence_id);
                buf.put_u32(all_data.len() as u32);
                for (rank, data) in all_data {
                    buf.put_u32(*rank);
                    buf.put_u32(data.len() as u32);
                    buf.put_slice(data);
                }
            }
            CoordMessage::ModexRequest {
                request_id,
                nspace,
                rank,
            } => {
                buf.put_u8(MessageType::ModexRequest as u8);
                buf.put_u64(*request_id);
                let nspace_bytes = nspace.as_bytes();
                buf.put_u16(nspace_bytes.len() as u16);
                buf.put_slice(nspace_bytes);
                buf.put_u32(*rank);
            }
            CoordMessage::ModexResponse { request_id, data } => {
                buf.put_u8(MessageType::ModexResponse as u8);
                buf.put_u64(*request_id);
                match data {
                    Some(d) => {
                        buf.put_u8(1); // has data
                        buf.put_u32(d.len() as u32);
                        buf.put_slice(d);
                    }
                    None => {
                        buf.put_u8(0); // no data
                    }
                }
            }
            CoordMessage::Ack { request_id } => {
                buf.put_u8(MessageType::Ack as u8);
                buf.put_u64(*request_id);
            }
            CoordMessage::Error {
                request_id,
                message,
            } => {
                buf.put_u8(MessageType::ErrorResponse as u8);
                buf.put_u64(*request_id);
                let msg_bytes = message.as_bytes();
                buf.put_u16(msg_bytes.len() as u16);
                buf.put_slice(msg_bytes);
            }
        }

        buf.freeze()
    }

    /// Deserialize message from bytes
    pub fn decode(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.is_empty() {
            return Err(ProtocolError::IncompletMessage);
        }

        let msg_type = MessageType::try_from(buf.get_u8())?;

        match msg_type {
            MessageType::FenceData => {
                if buf.remaining() < 16 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let fence_id = buf.get_u64();
                let rank = buf.get_u32();
                let data_len = buf.get_u32() as usize;
                if buf.remaining() < data_len {
                    return Err(ProtocolError::IncompletMessage);
                }
                let data = buf.copy_to_bytes(data_len);
                Ok(CoordMessage::FenceData {
                    fence_id,
                    rank,
                    data,
                })
            }
            MessageType::FenceComplete => {
                if buf.remaining() < 12 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let fence_id = buf.get_u64();
                let count = buf.get_u32() as usize;
                let mut all_data = Vec::with_capacity(count);
                for _ in 0..count {
                    if buf.remaining() < 8 {
                        return Err(ProtocolError::IncompletMessage);
                    }
                    let rank = buf.get_u32();
                    let data_len = buf.get_u32() as usize;
                    if buf.remaining() < data_len {
                        return Err(ProtocolError::IncompletMessage);
                    }
                    let data = buf.copy_to_bytes(data_len);
                    all_data.push((rank, data));
                }
                Ok(CoordMessage::FenceComplete { fence_id, all_data })
            }
            MessageType::ModexRequest => {
                if buf.remaining() < 10 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let request_id = buf.get_u64();
                let nspace_len = buf.get_u16() as usize;
                if buf.remaining() < nspace_len + 4 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let nspace_bytes = buf.copy_to_bytes(nspace_len);
                let nspace = String::from_utf8(nspace_bytes.to_vec())
                    .map_err(|_| ProtocolError::InvalidUtf8)?;
                let rank = buf.get_u32();
                Ok(CoordMessage::ModexRequest {
                    request_id,
                    nspace,
                    rank,
                })
            }
            MessageType::ModexResponse => {
                if buf.remaining() < 9 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let request_id = buf.get_u64();
                let has_data = buf.get_u8() != 0;
                let data = if has_data {
                    if buf.remaining() < 4 {
                        return Err(ProtocolError::IncompletMessage);
                    }
                    let data_len = buf.get_u32() as usize;
                    if buf.remaining() < data_len {
                        return Err(ProtocolError::IncompletMessage);
                    }
                    Some(buf.copy_to_bytes(data_len))
                } else {
                    None
                };
                Ok(CoordMessage::ModexResponse { request_id, data })
            }
            MessageType::Ack => {
                if buf.remaining() < 8 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let request_id = buf.get_u64();
                Ok(CoordMessage::Ack { request_id })
            }
            MessageType::ErrorResponse => {
                if buf.remaining() < 10 {
                    return Err(ProtocolError::IncompletMessage);
                }
                let request_id = buf.get_u64();
                let msg_len = buf.get_u16() as usize;
                if buf.remaining() < msg_len {
                    return Err(ProtocolError::IncompletMessage);
                }
                let msg_bytes = buf.copy_to_bytes(msg_len);
                let message =
                    String::from_utf8(msg_bytes.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)?;
                Ok(CoordMessage::Error {
                    request_id,
                    message,
                })
            }
        }
    }
}

/// Coordination server that handles incoming connections from peer pods
pub struct CoordServer {
    listener: TcpListener,
    message_tx: mpsc::UnboundedSender<(CoordMessage, SocketAddr)>,
}

impl CoordServer {
    /// Start the coordination server
    pub async fn bind(
        addr: &str,
    ) -> Result<(Self, mpsc::UnboundedReceiver<(CoordMessage, SocketAddr)>), ProtocolError> {
        let listener = TcpListener::bind(addr).await.map_err(ProtocolError::Io)?;
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        info!(addr, "Coordination server listening");

        Ok((
            Self {
                listener,
                message_tx,
            },
            message_rx,
        ))
    }

    /// Run the server, accepting connections
    pub async fn run(self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let tx = self.message_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, addr, tx).await {
                            warn!(error = %e, peer = %addr, "Connection error");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Accept error");
                }
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    tx: mpsc::UnboundedSender<(CoordMessage, SocketAddr)>,
) -> Result<(), ProtocolError> {
    debug!(peer = %addr, "New connection");

    loop {
        // Read message length (4 bytes)
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                debug!(peer = %addr, "Connection closed");
                return Ok(());
            }
            Err(e) => return Err(ProtocolError::Io(e)),
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Read message body
        let mut msg_buf = vec![0u8; msg_len];
        stream
            .read_exact(&mut msg_buf)
            .await
            .map_err(ProtocolError::Io)?;

        // Decode and forward message
        let msg = CoordMessage::decode(Bytes::from(msg_buf))?;
        let _ = tx.send((msg, addr));
    }
}

/// Send a message to a peer
pub async fn send_message(addr: &str, msg: &CoordMessage) -> Result<(), ProtocolError> {
    let mut stream = TcpStream::connect(addr).await.map_err(ProtocolError::Io)?;
    let encoded = msg.encode();

    // Write length prefix
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await
        .map_err(ProtocolError::Io)?;

    // Write message
    stream
        .write_all(&encoded)
        .await
        .map_err(ProtocolError::Io)?;

    stream.flush().await.map_err(ProtocolError::Io)?;

    Ok(())
}

/// Send a message and wait for a response
pub async fn send_and_receive(addr: &str, msg: &CoordMessage) -> Result<CoordMessage, ProtocolError> {
    let mut stream = TcpStream::connect(addr).await.map_err(ProtocolError::Io)?;
    let encoded = msg.encode();

    // Write length prefix
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await
        .map_err(ProtocolError::Io)?;

    // Write message
    stream
        .write_all(&encoded)
        .await
        .map_err(ProtocolError::Io)?;

    stream.flush().await.map_err(ProtocolError::Io)?;

    // Read response length
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(ProtocolError::Io)?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    // Read response body
    let mut msg_buf = vec![0u8; msg_len];
    stream
        .read_exact(&mut msg_buf)
        .await
        .map_err(ProtocolError::Io)?;

    CoordMessage::decode(Bytes::from(msg_buf))
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Incomplete message")]
    IncompletMessage,
    #[error("Invalid UTF-8 in message")]
    InvalidUtf8,
}
