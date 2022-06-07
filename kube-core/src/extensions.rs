#![allow(missing_docs)]

use crate::{
    params::ListParams,
    request::{Error, Request},
};

impl Request {
    pub fn list_with_version(&self, lp: &ListParams, version: &str) -> Result<http::Request<Vec<u8>>, Error> {
        let target = format!("{}?", self.url_path);
        let mut qp = form_urlencoded::Serializer::new(target);

        if let Some(fields) = &lp.field_selector {
            qp.append_pair("fieldSelector", fields);
        }
        if let Some(labels) = &lp.label_selector {
            qp.append_pair("labelSelector", labels);
        }
        if let Some(limit) = &lp.limit {
            qp.append_pair("limit", &limit.to_string());
        }
        if let Some(continue_token) = &lp.continue_token {
            qp.append_pair("continue", continue_token);
        }
        qp.append_pair("resourceVersion", version);

        let urlstr = qp.finish();
        let req = http::Request::get(urlstr);
        req.body(vec![]).map_err(Error::BuildRequest)
    }
}
