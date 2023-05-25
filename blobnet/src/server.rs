//! Server that makes blobnet accessible over HTTP.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::{self, Future};
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Instant;

use cadence_macros::{statsd_count, statsd_meter, statsd_time};
use hyper::header::HeaderValue;
use hyper::server::accept::Accept;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode, Version};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::headers::{HEADER_FILE_LENGTH, HEADER_RANGE, HEADER_SECRET};
use crate::provider::Provider;
use crate::utils::{body_stream, get_hash, stream_body};
use crate::{make_resp, BlobRange, Error, ReadStream};

/// Configuration for the file server.
pub struct Config {
    /// Provider that is used to handle requests.
    pub provider: Box<dyn Provider>,

    /// Secret that authorizes users to access the service.
    pub secret: String,
}

/// Create a file server listening at the given address.
pub async fn listen<I>(config: Config, incoming: I) -> hyper::Result<()>
where
    I: Accept,
    I::Error: Into<Box<dyn StdError + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    listen_with_shutdown(config, incoming, future::pending()).await
}

/// Create a file server listening at the given address, with graceful shutdown.
pub async fn listen_with_shutdown<I>(
    config: Config,
    incoming: I,
    shutdown: impl Future<Output = ()>,
) -> hyper::Result<()>
where
    I: Accept,
    I::Error: Into<Box<dyn StdError + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let config = Arc::new(config);

    // Low-level service boilerplate to interface with the [`hyper`] API.
    let make_svc = make_service_fn(move |_conn| {
        let config = Arc::clone(&config);
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let config = Arc::clone(&config);
                async {
                    let resp = handle(config, req).await;
                    Ok::<_, Infallible>(resp.unwrap_or_else(|err_resp| {
                        if err_resp.status() == StatusCode::INTERNAL_SERVER_ERROR {
                            println!(
                                "HTTP {} {:?} {:?}",
                                err_resp.status().as_str(),
                                err_resp.version(),
                                err_resp.body(),
                            );
                        }
                        err_resp
                    }))
                }
            }))
        }
    });

    Server::builder(incoming)
        .http2_max_frame_size(1 << 17)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown)
        .await
}

/// Main service handler for the file server.
async fn handle(config: Arc<Config>, req: Request<Body>) -> Result<Response<Body>, Response<Body>> {
    let http_version = match req.version() {
        Version::HTTP_09 => "http/0.9",
        Version::HTTP_10 => "http/1.0",
        Version::HTTP_11 => "http/1.1",
        Version::HTTP_2 => "http/2",
        Version::HTTP_3 => "http/3",
        _ => "<unknown>",
    };

    let method = match *req.method() {
        Method::GET => Method::GET.as_str(),
        Method::POST => Method::POST.as_str(),
        Method::PUT => Method::PUT.as_str(),
        Method::DELETE => Method::DELETE.as_str(),
        Method::HEAD => Method::HEAD.as_str(),
        _ => "<other>",
    };

    let start = Instant::now();
    statsd_count!(
        "server.requests_outstanding", 1,
        "http_version" => http_version,
        "http_method" => method);
    let guard = Arc::new(scopeguard::guard(AtomicU16::new(0), move |status| {
        let end = Instant::now();
        let latency = end - start;
        let status = status.load(SeqCst).to_string();
        statsd_count!(
            "server.requests_outstanding", -1,
            "http_version" => http_version,
            "http_method" => method);
        statsd_time!(
            "server.request_latency_ns", latency.as_nanos() as u64,
            "http_version" => http_version,
            "http_method" => method,
            "http_response_status" => status.as_str());
    }));

    let response = _handle(&config, req, guard.clone()).await;

    let response_status = match &response {
        Ok(response) => response.status(),
        Err(response) => response.status(),
    };

    guard.store(response_status.as_u16(), SeqCst);

    statsd_meter!(
        "server.requests", 1,
        "http_version" => http_version,
        "http_method" => method,
        "http_response_status" => response_status.as_str());

    response
}

async fn _handle<T>(
    config: &Arc<Config>,
    req: Request<Body>,
    guard: T,
) -> Result<Response<Body>, Response<Body>>
where
    T: Send + Unpin + 'static,
{
    let secret = req.headers().get(HEADER_SECRET);
    let secret = secret.and_then(|s| s.to_str().ok());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(make_resp(StatusCode::OK, "blobnet ok")),
        _ if secret != Some(&config.secret) => {
            Err(make_resp(StatusCode::UNAUTHORIZED, "unauthorized"))
        }
        (&Method::HEAD, path) => {
            let hash = get_hash(path)?;
            let len = config.provider.head(hash).await?;
            Response::builder()
                .header(HEADER_FILE_LENGTH, len.to_string())
                .body(Body::empty())
                .map_err(|e| Error::Internal(e.into()).into())
        }
        (&Method::GET, path) => {
            let range = req.headers().get(HEADER_RANGE).and_then(parse_range_header);
            let hash = get_hash(path)?;
            let reader = config.provider.get(hash, range).await?;
            Ok(Response::new(stream_body(Box::pin(
                InstrumentedStream::new(reader.into(), guard),
            ))))
        }
        (&Method::PUT, "/") => {
            let body = req.into_body();
            let hash = config.provider.put(body_stream(body)).await?;
            Ok(Response::new(Body::from(hash)))
        }
        _ => Err(make_resp(StatusCode::NOT_FOUND, "invalid request path")),
    }
}

/// Parse an HTTP Range header of the format "X-Bn-Range: <start>-<end>".
///
/// The start index is inclusive, and the end index is exclusive. This differs
/// from the standard HTTP `Range` header, which has both ends inclusive.
fn parse_range_header(s: &HeaderValue) -> BlobRange {
    let s = s.to_str().ok()?;
    let (start, end) = s.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

struct InstrumentedStream<'a, T>
where
    T: Unpin,
{
    inner: ReadStream<'a>,
    _guard: T,
}

impl<'a, T> InstrumentedStream<'a, T>
where
    T: Unpin,
{
    fn new(inner: ReadStream<'a>, guard: T) -> Self {
        Self {
            inner,
            _guard: guard,
        }
    }
}

impl<T> AsyncRead for InstrumentedStream<'_, T>
where
    T: Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.get_mut().inner.as_mut().poll_read(cx, buf)
    }
}
