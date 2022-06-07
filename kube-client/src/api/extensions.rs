#![allow(missing_docs)]

use crate::{api::Api, Error, Result};
use kube_core::{params::ListParams, ObjectList};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

impl<K> Api<K>
where
    K: Clone + DeserializeOwned + Debug,
{
    pub async fn list_with_version(&self, lp: &ListParams, version: &str) -> Result<ObjectList<K>> {
        let mut req = self
            .request
            .list_with_version(lp, version)
            .map_err(Error::BuildRequest)?;
        req.extensions_mut().insert("list");
        self.client.request::<ObjectList<K>>(req).await
    }
}
