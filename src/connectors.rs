use serde::{Serialize, Deserialize};

use crate::{KafkaConnectorError, get_connect_host, KafkaConnector};



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

impl KafkaConnector {
    pub async fn get_connectors(&self) -> Result<Vec<String>, KafkaConnectorError> {
        return self.easy_get_request::<Vec<String>>("connectors".to_string()).await;
    }

    pub async fn get_connectors_status(&self, name: String) -> Result<GetConnectorsStatus, KafkaConnectorError> {
        return self.easy_get_request::<GetConnectorsStatus>(format!("connectors/{}/status", name)).await;
    }

    pub async fn get_connectors_info(&self, name: String) -> Result<PostConnectorsResponse, KafkaConnectorError> {
        return self.easy_get_request::<PostConnectorsResponse>(format!("connectors/{}", name).to_string()).await;
    }


    pub async fn post_connector(&self, post_connector_request: PostConnectorsRequest) -> Result<PostConnectorsResponse, KafkaConnectorError> {
        self.easy_post_request("connectors".to_string(), post_connector_request).await
    }

    pub async fn put_connectors_config(&self, name: String, config: serde_json::Value) -> Result<PutConnectorsConfigResponse, KafkaConnectorError> {
        let path = format!("{}/connectors/{}/config", self.host, name);
        let cclient = self.client.clone();
        let r = cclient.put(path)
            .json(&config)
            .send()
            .await.map_err(|e| { KafkaConnectorError::RequestError(e.into()) } )?;
        let o = r.json::<PutConnectorsConfigResponse>().await.map_err(|e| { KafkaConnectorError::FormatError(e.into()) })?;
        return Ok(o);
    }


    pub async fn pause_connector(&self, name: String) -> Result<(), KafkaConnectorError> {
        let path = format!("{}/{}/pause", "connectors", name);
        self.easy_put_request(path).await
    }

    pub async fn resume_connector(&self, name: String) -> Result<(), KafkaConnectorError> {
        let path = format!("{}/{}/resume", "connectors", name);
        self.easy_put_request(path).await
    }

    pub async fn delete_connector(&self, name: String) -> Result<(), KafkaConnectorError> {
        let path = format!("{}/{}/{}", self.host, "connectors", name);
        let cclient = self.client.clone();
        cclient.delete(path).send()
            .await.map_err(|e| { KafkaConnectorError::RequestError(e.into())} )?;
        return Ok(());
    }
}
