//! The HTTP servers of `csi --metrics-address` and `locality`.

use std::convert::Infallible;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use rustix::io::Errno;
use tokio::net::{TcpListener, TcpSocket};
use tracing::{debug, warn};

/// Binds `address`. An IPv6 address takes IPv4 too, whatever
/// `net.ipv6.bindv6only` says. A kernel booted with `ipv6.disable=1` has no
/// IPv6 sockets, so there `[::]` falls back to `0.0.0.0`.
pub async fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => return TcpListener::bind(address).await,
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    };
    let socket = match socket {
        Err(e)
            if address.ip() == Ipv6Addr::UNSPECIFIED
                && e.raw_os_error() == Some(Errno::AFNOSUPPORT.raw_os_error()) =>
        {
            return TcpListener::bind((Ipv4Addr::UNSPECIFIED, address.port())).await;
        }
        socket => socket?,
    };
    socket2::SockRef::from(&socket).set_only_v6(false)?;
    // Like `TcpListener::bind`, so a restart can bind during TIME_WAIT.
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    socket.listen(1024)
}

pub fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    let value = header::HeaderValue::from_static(content_type);
    response.headers_mut().insert(header::CONTENT_TYPE, value);
    response
}

/// Answers every connection on `listener` with `respond`. `what` names the
/// server in logs.
pub async fn serve<F, R>(listener: TcpListener, what: &'static str, respond: F)
where
    F: Fn(Request<Incoming>) -> R + Clone + Send + 'static,
    R: Future<Output = Response<Full<Bytes>>> + Send + 'static,
{
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Errors here, such as running out of file descriptors, would
                // recur on an immediate retry.
                warn!("accepting a {what} connection: {e}; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let respond = respond.clone();
        let service = service_fn(move |request| {
            let response = respond(request);
            async move { Ok::<_, Infallible>(response.await) }
        });
        tokio::spawn(async move {
            // The timer enforces hyper's default 30 second limit on reading
            // the request head, so idle connections don't pile up.
            if let Err(e) = http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!("serving {what}: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chart serves metrics on `[::]`, which has to take IPv4 too. The
    /// default `net.ipv6.bindv6only=0` allows that even without the socket
    /// option, so this catches a regression only on a host or in a network
    /// namespace with `net.ipv6.bindv6only=1`.
    #[tokio::test]
    async fn listens_on_ipv4_too() {
        let listener = listen("[::]:0".parse().unwrap()).await.unwrap();
        assert!(!socket2::SockRef::from(&listener).only_v6().unwrap());
        let port = listener.local_addr().unwrap().port();
        tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
    }
}
