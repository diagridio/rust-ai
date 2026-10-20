//! A [`WorkflowClient`] that can reach Diagrid Catalyst, not only a local sidecar.
//!
//! The Dapr Rust SDK (`dapr` 0.19.0) dials its sidecar in plaintext and sends no
//! API token. That suits a local `dapr run` sidecar, but Catalyst's endpoint
//! (`DAPR_GRPC_ENDPOINT=https://grpc-prj<id>.cloud.r1.diagrid.io:443` under
//! `diagrid dev run`) needs TLS, and every call has to carry the app's
//! `DAPR_API_TOKEN`: with the SDK alone the connection fails with a transport
//! error.
//!
//! Until the SDK does both itself, [`workflow_client`] puts a relay in between
//! when one is needed: a loopback HTTP/2 listener that forwards every gRPC call
//! to the real endpoint, over TLS if that endpoint asks for it, with the token
//! added if there is one. The SDK talks plaintext to the relay and nothing else
//! changes. Against a plain local sidecar there is no relay at all.

use dapr::client::config::{default_sidecar_address, DAPR_API_TOKEN_ENV};
use dapr::workflow::{WorkflowClient, WorkflowError};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tonic::transport::{ClientTlsConfig, Endpoint};
use tower::ServiceExt;

/// The metadata key Dapr reads the API token from.
const API_TOKEN_HEADER: &str = "dapr-api-token";

/// Connect a workflow client to the sidecar the environment names, whether that
/// is a local `dapr run` sidecar or a Catalyst project.
///
/// Reads the same variables as [`WorkflowClient::new`] (`DAPR_GRPC_ENDPOINT`,
/// then `DAPR_GRPC_PORT`) plus `DAPR_API_TOKEN`, and relays through loopback
/// only when the endpoint needs TLS (`https://`, or `?tls=true`) or a token has
/// to be sent. Use it in place of `WorkflowClient::new()`:
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// let mut client = diagrid_goose::workflow_client().await?;
/// diagrid_goose::register(client.registry_mut());
/// # Ok(())
/// # }
/// ```
pub async fn workflow_client() -> dapr::workflow::Result<WorkflowClient> {
    let token = std::env::var(DAPR_API_TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty());
    match route(&default_sidecar_address(), token.as_deref())? {
        Route::Direct => WorkflowClient::new().await,
        Route::Relay { upstream, tls } => {
            let tls = tls.then(|| ClientTlsConfig::new().with_webpki_roots());
            let relay = start_relay(upstream, tls, token).await?;
            WorkflowClient::new_with_address(relay).await
        }
    }
}

/// How to reach the sidecar at `address`.
#[derive(Debug, PartialEq)]
enum Route {
    /// The SDK can dial it as is: plaintext, no token.
    Direct,
    /// Through a loopback relay to `upstream`, adding TLS when `tls` is set and
    /// the API token whenever there is one.
    Relay { upstream: String, tls: bool },
}

/// Decide how to reach `address`, given the API token (if any). TLS follows
/// Dapr's endpoint convention: an `https` scheme or a `tls=true` query. The
/// query is not part of the address that is dialled.
fn route(address: &str, token: Option<&str>) -> dapr::workflow::Result<Route> {
    let address = if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    };
    let uri: http::Uri = address
        .parse()
        .map_err(|e| WorkflowError::InvalidAddress(format!("{address}: {e}")))?;
    let authority = uri
        .authority()
        .ok_or_else(|| WorkflowError::InvalidAddress(format!("{address}: no host")))?;
    let tls = uri.scheme_str() == Some("https")
        || uri
            .query()
            .is_some_and(|query| query.split('&').any(|pair| pair == "tls=true"));
    if !tls && token.is_none() {
        return Ok(Route::Direct);
    }
    let scheme = if tls { "https" } else { "http" };
    Ok(Route::Relay {
        upstream: format!("{scheme}://{authority}"),
        tls,
    })
}

/// Listen on a free loopback port and forward every request to `upstream`,
/// with `token` added as `dapr-api-token`. Returns the address to dial.
///
/// A tonic `Channel` makes the upstream calls, so HTTP/2 multiplexing,
/// streaming bodies and gRPC trailers pass through untouched. That matters for
/// the workflow worker, which holds a long-lived stream open to receive work.
async fn start_relay(
    upstream: String,
    tls: Option<ClientTlsConfig>,
    token: Option<String>,
) -> dapr::workflow::Result<String> {
    let mut endpoint = Endpoint::from_shared(upstream.clone())
        .map_err(|e| WorkflowError::InvalidAddress(format!("{upstream}: {e}")))?;
    if let Some(tls) = tls {
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| WorkflowError::ConnectionFailed(format!("{upstream}: {e}")))?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| WorkflowError::ConnectionFailed(format!("{upstream}: {e}")))?;
    let token = token
        .map(|token| http::HeaderValue::from_str(&token))
        .transpose()
        .map_err(|_| {
            WorkflowError::Other(format!("{DAPR_API_TOKEN_ENV} is not a valid header value"))
        })?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| WorkflowError::ConnectionFailed(format!("relay listener: {e}")))?;
    let address = listener
        .local_addr()
        .map_err(|e| WorkflowError::ConnectionFailed(format!("relay listener: {e}")))?;
    tokio::spawn(async move {
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    log::warn!("relay to {upstream}: accept failed: {e}");
                    continue;
                }
            };
            let channel = channel.clone();
            let token = token.clone();
            let upstream = upstream.clone();
            tokio::spawn(async move {
                let relay = service_fn(move |mut request: Request<Incoming>| {
                    if let Some(token) = &token {
                        request
                            .headers_mut()
                            .insert(API_TOKEN_HEADER, token.clone());
                    }
                    // The channel replaces the loopback scheme and authority
                    // with the upstream's, and keeps the gRPC method path.
                    channel.clone().oneshot(request.map(tonic::body::Body::new))
                });
                // Plaintext HTTP/2 with prior knowledge, which is what the SDK
                // speaks to an `http://` address.
                if let Err(e) = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), relay)
                    .await
                {
                    log::warn!("relay to {upstream}: connection ended: {e}");
                }
            });
        }
    });
    Ok(format!("http://{address}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay(upstream: &str, tls: bool) -> Route {
        Route::Relay {
            upstream: upstream.to_string(),
            tls,
        }
    }

    #[test]
    fn plain_local_sidecar_is_direct() {
        assert_eq!(
            route("http://127.0.0.1:50001", None).unwrap(),
            Route::Direct
        );
        assert_eq!(route("127.0.0.1:50001", None).unwrap(), Route::Direct);
    }

    #[test]
    fn catalyst_endpoint_relays_with_tls() {
        assert_eq!(
            route("https://grpc-prj1.cloud.r1.diagrid.io:443", Some("t")).unwrap(),
            relay("https://grpc-prj1.cloud.r1.diagrid.io:443", true)
        );
    }

    #[test]
    fn tls_query_relays_with_tls_and_is_not_dialled() {
        assert_eq!(
            route("http://sidecar:443?tls=true", None).unwrap(),
            relay("https://sidecar:443", true)
        );
        assert_eq!(
            route("https://sidecar:443?tls=true", None).unwrap(),
            relay("https://sidecar:443", true)
        );
    }

    #[test]
    fn token_on_plaintext_sidecar_relays_without_tls() {
        assert_eq!(
            route("http://127.0.0.1:50001", Some("t")).unwrap(),
            relay("http://127.0.0.1:50001", false)
        );
    }

    #[test]
    fn tls_without_token_still_relays() {
        assert_eq!(
            route("https://sidecar:443", None).unwrap(),
            relay("https://sidecar:443", true)
        );
    }

    #[test]
    fn address_without_host_is_rejected() {
        assert!(route("http://", None).is_err());
    }

    /// The relay itself, against a stand-in sidecar: a gRPC server that refuses
    /// any call without the token, the way Catalyst does.
    mod relay {
        use std::net::SocketAddr;
        use std::time::Duration;

        use tokio_stream::wrappers::TcpListenerStream;
        use tonic::transport::{Certificate, Channel, Identity, Server, ServerTlsConfig};
        use tonic::{Code, Status};
        use tonic_health::pb::health_check_response::ServingStatus;
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;
        use tonic_health::server::HealthReporter;

        use super::super::*;

        const TOKEN: &str = "test-token";

        fn require_token(request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
            match request.metadata().get(API_TOKEN_HEADER) {
                Some(token) if token == TOKEN => Ok(request),
                _ => Err(Status::unauthenticated("missing dapr-api-token")),
            }
        }

        /// Serve gRPC health checks on a free port, over TLS if `identity` is
        /// given. Keep the reporter: dropping it ends health watch streams.
        async fn sidecar(identity: Option<Identity>) -> (SocketAddr, HealthReporter) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (reporter, health) = tonic_health::server::health_reporter();
            let mut server = Server::builder();
            if let Some(identity) = identity {
                server = server
                    .tls_config(ServerTlsConfig::new().identity(identity))
                    .unwrap();
            }
            let router = server
                .layer(tonic::service::InterceptorLayer::new(require_token))
                .add_service(health);
            tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
            (address, reporter)
        }

        async fn health_client(address: String) -> HealthClient<Channel> {
            HealthClient::new(
                Channel::from_shared(address)
                    .unwrap()
                    .connect()
                    .await
                    .unwrap(),
            )
        }

        #[tokio::test]
        async fn plaintext_relay_adds_the_token() {
            let (address, _reporter) = sidecar(None).await;

            // Without the relay the sidecar refuses the call, so the pass below
            // is the relay's doing.
            let refused = health_client(format!("http://{address}"))
                .await
                .check(HealthCheckRequest::default())
                .await
                .unwrap_err();
            assert_eq!(refused.code(), Code::Unauthenticated);

            let relay = start_relay(format!("http://{address}"), None, Some(TOKEN.into()))
                .await
                .unwrap();
            let reply = health_client(relay)
                .await
                .check(HealthCheckRequest::default())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.status(), ServingStatus::Serving);
        }

        #[tokio::test]
        async fn tls_relay_adds_the_token_and_streams() {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let cert_pem = cert.cert.pem();
            let key_pem = cert.signing_key.serialize_pem();
            let (address, _reporter) = sidecar(Some(Identity::from_pem(&cert_pem, &key_pem))).await;

            let tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&cert_pem))
                .domain_name("localhost");
            let relay = start_relay(format!("https://{address}"), Some(tls), Some(TOKEN.into()))
                .await
                .unwrap();
            let mut client = health_client(relay).await;

            let reply = client
                .check(HealthCheckRequest::default())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.status(), ServingStatus::Serving);

            // A server stream through the relay, the shape of the workflow
            // worker's long-lived work-item stream.
            let mut updates = client
                .watch(HealthCheckRequest::default())
                .await
                .unwrap()
                .into_inner();
            let first = tokio::time::timeout(Duration::from_secs(5), updates.message())
                .await
                .expect("no message on the stream within 5s")
                .unwrap()
                .unwrap();
            assert_eq!(first.status(), ServingStatus::Serving);
        }
    }
}
