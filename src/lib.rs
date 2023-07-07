use std::{error, fmt::Display};

use log::{debug, error};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

pub mod connectors;
pub mod common;
pub mod tasks;
#[cfg(test)]
mod tests;

const HOST_ENV: &'static str = "KAFKA_CONNECTOR_HOST";

#[derive(Debug)]
pub enum KafkaConnectorError {
   HostUndefined,
   RequestError(Box<dyn error::Error>),
   FormatError(Box<dyn error::Error>),
   CustomFormatError
}

impl Display for KafkaConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let o = format!("{:#?}", self);
        return f.write_str(&o);
    }
}

pub fn get_connect_host() -> Result<String, KafkaConnectorError> {
    match std::env::var(HOST_ENV) {
        Ok(v) => { Ok(v) },
        Err(_) => {  Err(KafkaConnectorError::HostUndefined) },
    }
}


fn create_error<T>(e: Box<dyn error::Error>) -> Result<T, KafkaConnectorError> {
    return Err(KafkaConnectorError::RequestError(e));
}

async fn easy_get_request<T: Serialize + DeserializeOwned>(path: String) -> Result<T, KafkaConnectorError> {
    let path = format!("{}/{}", get_connect_host()?, path);
    let o = reqwest::get(path).await.map_err(|e| { KafkaConnectorError::RequestError(e.into()) })?;
    let r = o.json::<T>().await.map_err(|e| { KafkaConnectorError::FormatError(e.into()) })?;
    return Ok(r);
}



pub async fn easy_post_request<Q: Serialize + DeserializeOwned, R: Serialize + DeserializeOwned>(path: String, body: Q) -> Result<R, KafkaConnectorError> {
    let path = format!("{}/{}", get_connect_host()?, path);
    let client = reqwest::Client::new();
    let res = client.post(path)
        .json(&body)
        .send()
        .await.map_err(|e| { KafkaConnectorError::RequestError(e.into())} )?;
    let r = res.json::<R>().await.map_err(|e| KafkaConnectorError::FormatError(e.into()))?;
    return Ok(r);
}


pub async fn easy_put_request(path: String) -> Result<(), KafkaConnectorError> {
    let client = reqwest::Client::new();
    client.put(path).send()
        .await.map_err(|e| { KafkaConnectorError::RequestError(e.into())} )?;
    return Ok(());
}
