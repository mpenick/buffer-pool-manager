use crate::buffer_pool::{BufferPoolManager, DiskManagerMock};
use hyper::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::runtime;

fn page_param(req: Request<Body>) -> Option<i32> {
    if let Some(query) = req.uri().query() {
        for pair in query.split("&") {
            let kv: Vec<&str> = pair.split("=").collect();
            if kv.len() == 2 && kv[0].eq("page") {
                if let Ok(page) = kv[1].parse::<i32>() {
                    return Some(page);
                }
            }
        }
    }
    None
}

fn make_error(status: StatusCode, response: &mut Response<Body>) {
    *response.status_mut() = status;
    *response.body_mut() = Body::from("{}");
}

fn add_headers(response: &mut Response<Body>) {
    response
        .headers_mut()
        .append(CONTENT_TYPE, "application/json".parse().unwrap());
    response
        .headers_mut()
        .append(ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
}

fn new_page(bpm: Arc<Mutex<BufferPoolManager>>, response: &mut Response<Body>) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Err(_) = m.new_page() {
        make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
    } else {
        *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
    }
}

fn flush_page(
    bpm: Arc<Mutex<BufferPoolManager>>,
    req: Request<Body>,
    response: &mut Response<Body>,
) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Some(page) = page_param(req) {
        if let Err(_) = m.flush_page(page) {
            make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
        } else {
            *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
        }
    } else {
        make_error(StatusCode::BAD_REQUEST, response);
    }
}

fn delete_page(
    bpm: Arc<Mutex<BufferPoolManager>>,
    req: Request<Body>,
    response: &mut Response<Body>,
) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Some(page) = page_param(req) {
        if let Err(_) = m.delete_page(page) {
            make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
        } else {
            *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
        }
    } else {
        make_error(StatusCode::BAD_REQUEST, response);
    }
}

fn unpin_page(
    bpm: Arc<Mutex<BufferPoolManager>>,
    req: Request<Body>,
    response: &mut Response<Body>,
) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Some(page) = page_param(req) {
        if let Err(_) = m.unpin_page(page, false) {
            make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
        } else {
            *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
        }
    } else {
        make_error(StatusCode::BAD_REQUEST, response);
    }
}

fn fetch_page(
    bpm: Arc<Mutex<BufferPoolManager>>,
    req: Request<Body>,
    response: &mut Response<Body>,
) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Some(page) = page_param(req) {
        if let Err(_) = m.fetch_page(page) {
            make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
        } else {
            *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
        }
    } else {
        make_error(StatusCode::BAD_REQUEST, response);
    }
}

fn flush_all(bpm: Arc<Mutex<BufferPoolManager>>, response: &mut Response<Body>) {
    add_headers(response);
    let mut m = bpm.lock().unwrap();
    if let Err(_) = m.flush_all_pages() {
        make_error(StatusCode::INTERNAL_SERVER_ERROR, response);
    } else {
        *response.body_mut() = Body::from(serde_json::to_string(&m.response()).unwrap());
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

pub fn serve() {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run_server());
}

async fn route(
    req: Request<Body>,
    bpm: Arc<Mutex<BufferPoolManager>>,
) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/new") => new_page(bpm, &mut response),
        (&Method::GET, "/flush") => flush_page(bpm, req, &mut response),
        (&Method::GET, "/delete") => delete_page(bpm, req, &mut response),
        (&Method::GET, "/unpin") => unpin_page(bpm, req, &mut response),
        (&Method::GET, "/fetch") => fetch_page(bpm, req, &mut response),
        (&Method::GET, "/flush-all") => flush_all(bpm, &mut response),
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    };

    Ok(response)
}

async fn run_server() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let shared = Arc::new(Mutex::new(BufferPoolManager::new(DiskManagerMock::new())));

    let svc = make_service_fn(move |_| {
        let local = shared.clone();
        async { Ok::<_, Infallible>(service_fn(move |req| route(req, local.clone()))) }
    });

    let server = Server::bind(&addr).serve(svc);
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
}
