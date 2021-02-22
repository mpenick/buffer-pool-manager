use crate::buffer_pool;
use crate::buffer_pool::BufferPoolManager;
use hyper::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::runtime;

fn add_headers(response: &mut Response<Body>) {
    response
        .headers_mut()
        .append(CONTENT_TYPE, "application/json".parse().unwrap());
    response
        .headers_mut()
        .append(ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
}

fn new_page(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

fn flush_page(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

fn delete_page(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

fn unpin_page(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

fn fetch_page(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

fn flush_all(bpm: &BufferPoolManager, req: Request<Body>, response: &mut Response<Body>) {
    add_headers(response);
    *response.body_mut() = Body::from(serde_json::to_string(&bpm.response()).unwrap());
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

async fn service_request(bpm: &BufferPoolManager, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/new") => new_page(bpm, req, &mut response),
        (&Method::GET, "/flush") => flush_page(bpm, req, &mut response),
        (&Method::GET, "/delete") => delete_page(bpm, req, &mut response),
        (&Method::GET, "/unpin") => unpin_page(bpm, req, &mut response),
        (&Method::GET, "/fetch") => fetch_page(bpm, req, &mut response),
        (&Method::GET, "/flush-all") => flush_all(bpm, req, &mut response),
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    };

    Ok(response)
}

pub fn serve(bpm: &mut BufferPoolManager) {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run_server());
}

async fn run_server() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(service_request)) });

    let server = Server::bind(&addr).serve(make_svc);
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
}
