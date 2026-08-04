use std::{env, error::Error};

use axum::{Json, Router, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    status: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let address = env::var("STORYA_API_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = TcpListener::bind(&address).await?;
    let app = Router::new().route("/health", get(health));

    info!(%address, "storya-api listening");
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "storya-api",
        status: "ok",
    })
}
