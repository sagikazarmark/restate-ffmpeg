use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::restate_config::*;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub restate: RestateConfig,

    #[serde(default, alias = "profile")]
    pub profiles: HashMap<String, HashMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct RestateConfig {
    #[serde(default)]
    pub service: ServiceOptionsConfig,
}
