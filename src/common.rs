use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{KafkaConnectorError, easy_get_request};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectClusterResponse {
    pub version: String,
    pub commit: String,
    pub kafka_cluster_id: String
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetConnectorPluginsResponse {
    pub class: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub topics: Vec<String>
}


pub async fn connect_cluster() -> Result<ConnectClusterResponse, KafkaConnectorError> {
    return easy_get_request::<ConnectClusterResponse>("".to_string()).await;
}

pub async fn get_connector_plugins() -> Result<GetConnectorPluginsResponse, KafkaConnectorError> {
    return easy_get_request::<GetConnectorPluginsResponse>("connector-plugins".to_string()).await;
}

pub async fn get_connectors_topics(name: String) -> Result<Vec<String>, KafkaConnectorError> {
    let res = easy_get_request::<serde_json::Value>(format!("connectors/{}/topics", name)).await?;
    match res.get(name) {
        Some(topics) => {
            match topics.get("topics") {
                Some(topics) => {
                    let topics = serde_json::from_value::<Vec<String>>(topics.clone());
                    match topics {
                        Ok(topics) => Ok(topics),
                        Err(e) => Err(KafkaConnectorError::FormatError(e.into())),
                    }
                },
                None => Err(KafkaConnectorError::FormatError(Box::<dyn std::error::Error>::from(KafkaConnectorError::CustomFormatError.to_string()))),
            }
        },
        None => Err(KafkaConnectorError::FormatError(Box::<dyn std::error::Error>::from(KafkaConnectorError::CustomFormatError.to_string()))),
    }
}

