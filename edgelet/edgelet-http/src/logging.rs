// Copyright (c) Microsoft. All rights reserved.
#![allow(deprecated)]

use std::error::Error as StdError;

use chrono::prelude::*;
use edgelet_core::pid::Pid;
use futures::future;
use futures::prelude::*;
use http::header::{CONTENT_LENGTH, USER_AGENT};
use hyper::service::{NewService, Service};
use hyper::{Body, Request, Response};

#[derive(Clone)]
pub struct LoggingService<T> {
    label: String,
    inner: T,
}

impl<T> LoggingService<T> {
    pub fn new(label: String, inner: T) -> Self {
        LoggingService { label, inner }
    }
}

pub struct ResponseFuture<T> {
    inner: T,
    label: String,
    request: String,
    user_agent: String,
    pid: Option<Pid>,
}

impl<T> Future for ResponseFuture<T>
where
    T: Future<Item = Response<Body>>,
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let response = try_ready!(self.inner.poll());

        let body_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|l| l.to_str().ok().map(|l| l.to_string()))
            .unwrap_or_else(|| "-".to_string());
        let pid = self
            .pid
            .as_ref()
            .map_or_else(|| "-".to_string(), |p| p.to_string());

        info!(
            "[{}] - - - [{}] \"{}\" {} {} \"-\" \"{}\" pid({})",
            self.label,
            Utc::now(),
            self.request,
            response.status(),
            body_length,
            self.user_agent,
            pid,
        );
        Ok(Async::Ready(response))
    }
}

impl<T> Service for LoggingService<T>
where
    T: Service<ResBody = Body>,
{
    type ReqBody = T::ReqBody;
    type ResBody = T::ResBody;
    type Error = T::Error;
    type Future = ResponseFuture<T::Future>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let uri = req.uri().query().map_or_else(
            || req.uri().path().to_string(),
            |q| format!("{}?{}", req.uri().path(), q),
        );
        let request = format!("{} {} {:?}", req.method(), uri, req.version());
        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .and_then(|ua| ua.to_str().ok())
            .unwrap_or_else(|| "-")
            .to_string();
        let pid = req.extensions().get::<Pid>().cloned();

        let inner = self.inner.call(req);
        ResponseFuture {
            label: self.label.clone(),
            inner,
            request,
            user_agent,
            pid,
        }
    }
}

impl<T> NewService for LoggingService<T>
where
    T: Clone + Service<ResBody = Body>,
{
    type ReqBody = <Self::Service as Service>::ReqBody;
    type ResBody = <Self::Service as Service>::ResBody;
    type Error = <Self::Service as Service>::Error;
    type Service = Self;
    type Future = future::FutureResult<Self::Service, Self::InitError>;
    type InitError = Box<StdError + Send + Sync>;

    fn new_service(&self) -> Self::Future {
        future::ok(self.clone())
    }
}
