use serde::{Serialize, Deserialize};

use crate::{KafkaConnectorError, KafkaConnector};

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

impl KafkaConnector {
    pub async fn connect_cluster(&self) -> Result<ConnectClusterResponse, KafkaConnectorError> {
        return self.easy_get_request::<ConnectClusterResponse>("".to_string()).await;
    }

    pub async fn get_connector_plugins(&self) -> Result<GetConnectorPluginsResponse, KafkaConnectorError> {
        return self.easy_get_request::<GetConnectorPluginsResponse>("connector-plugins".to_string()).await;
    }

    pub async fn get_connectors_topics(&self, name: String) -> Result<Vec<String>, KafkaConnectorError> {
        let res = self.easy_get_request::<serde_json::Value>(format!("connectors/{}/topics", name)).await?;
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
}
