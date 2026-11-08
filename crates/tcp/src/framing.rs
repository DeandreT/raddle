use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;

pub(crate) const MAGIC: &[u8; 5] = b"RDTC\x01";
pub(crate) const SELECTED_MAGIC: &[u8; 5] = b"RDTC\x02";
pub(crate) const ACCEPTED: u8 = 0;
pub(crate) const RETRY: u8 = 1;
pub(crate) const REJECTED: u8 = 2;
pub(crate) const REPLY: u8 = 3;

pub(crate) async fn read_frame<R: AsyncRead + Unpin>(
    input: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let length = input.read_u32().await.map_err(|_| Error::Uncertain)? as usize;
    if length == 0 || length > max_bytes {
        // Untrusted oversized frames are not drained or allocated. The connection
        // is closed, so their bytes can never be mistaken for another message.
        return Err(Error::Rejected);
    }
    let mut bytes = vec![0; length];
    input
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Error::Uncertain)?;
    Ok(bytes)
}

pub(crate) async fn write_frame<W: AsyncWrite + Unpin>(
    output: &mut W,
    bytes: &[u8],
) -> Result<(), Error> {
    let length = u32::try_from(bytes.len()).map_err(|_| Error::Rejected)?;
    output
        .write_u32(length)
        .await
        .map_err(|_| Error::Uncertain)?;
    output.write_all(bytes).await.map_err(|_| Error::Uncertain)
}
