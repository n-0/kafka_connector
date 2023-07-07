use std::{error};

use log::{debug, error};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{easy_get_request, KafkaConnectorError, easy_post_request, easy_put_request, get_connect_host};



#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetConnectorsResponse {
    pub connectors: Vec<String>
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorTask {
    pub connector: String,
    pub task: i32
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostConnectorsRequest {
    pub name: String,
    pub config: serde_json::Value,
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostConnectorsResponse {
    pub name: String,
    pub config: serde_json::Value,
    pub tasks: Vec<ConnectorTask>
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetConnectorsStatus {
    pub name: String,
    pub connector: serde_json::Value,
    pub tasks: Vec<serde_json::Value>
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PutConnectorsConfigResponse {
    pub name: String,
    pub config: serde_json::Value,
    pub tasks: Vec<ConnectorTask>
}


pub async fn get_connectors() -> Result<Vec<String>, KafkaConnectorError> {
    return easy_get_request::<Vec<String>>("connectors".to_string()).await;
}

pub async fn get_connectors_status(name: String) -> Result<GetConnectorsStatus, KafkaConnectorError> {
    return easy_get_request::<GetConnectorsStatus>(format!("connectors/{}/status", name)).await;
}

pub async fn get_connectors_info(name: String) -> Result<PostConnectorsResponse, KafkaConnectorError> {
    return easy_get_request::<PostConnectorsResponse>(format!("connectors/{}", name).to_string()).await;
}


pub async fn post_connector(post_connector_request: PostConnectorsRequest) -> Result<PostConnectorsResponse, KafkaConnectorError> {
    easy_post_request("connectors".to_string(), post_connector_request).await
}

pub async fn put_connectors_config(name: String, config: serde_json::Value) -> Result<PutConnectorsConfigResponse, KafkaConnectorError> {
    let path = format!("{}/connectors/{}/config", get_connect_host()?, name);
    let client = reqwest::Client::new();
    let r = client.put(path)
        .json(&config)
        .send()
        .await.map_err(|e| { KafkaConnectorError::RequestError(e.into()) } )?;
    let o = r.json::<PutConnectorsConfigResponse>().await.map_err(|e| { KafkaConnectorError::FormatError(e.into()) })?;
    return Ok(o);
}


pub async fn pause_connector(name: String) -> Result<(), KafkaConnectorError> {
    let path = format!("{}/{}/{}/pause", get_connect_host()?, "connectors", name);
    easy_put_request(path).await
}

pub async fn resume_connector(name: String) -> Result<(), KafkaConnectorError> {
    let path = format!("{}/{}/{}/resume", get_connect_host()?, "connectors", name);
    easy_put_request(path).await
}

pub async fn delete_connector(name: String) -> Result<(), KafkaConnectorError> {
    let path = format!("{}/{}/{}", get_connect_host()?, "connectors", name);
    let client = reqwest::Client::new();
    client.put(path).send()
        .await.map_err(|e| { KafkaConnectorError::RequestError(e.into())} )?;
    return Ok(());
}
