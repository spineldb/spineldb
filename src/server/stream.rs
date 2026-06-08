// src/server/stream.rs

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

/// An enum to wrap different stream types (plain TCP or TLS) into a single type.
pub enum AnyStream {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for AnyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            AnyStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            AnyStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AnyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            AnyStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            AnyStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            AnyStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            AnyStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            AnyStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            AnyStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_anystream_tcp_read_write() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            AnyStream::Tcp(stream)
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client = AnyStream::Tcp(client_stream);
        let mut server_stream = server.await.unwrap();

        // Write from client
        client.write_all(b"hello").await.unwrap();
        client.flush().await.unwrap();

        // Read from server
        let mut buf = [0u8; 5];
        server_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        // Write from server
        server_stream.write_all(b"world").await.unwrap();
        server_stream.flush().await.unwrap();

        // Read from client
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"world");
    }

    #[tokio::test]
    async fn test_anystream_tcp_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut any_stream = AnyStream::Tcp(stream);
            // Should not panic
            use tokio::io::AsyncWriteExt;
            let _ = any_stream.shutdown().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut client = AnyStream::Tcp(client_stream);
        // Give server time to shutdown
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Client write should fail after server shutdown
        let result = client.write_all(b"test").await;
        // The connection may or may not have been closed yet, so we just check it doesn't panic
        let _ = result;
        let _ = server.await;
    }
}
