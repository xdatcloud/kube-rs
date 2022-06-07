use super::watcher::{Error, Event, Result};
use derivative::Derivative;
use futures::{stream::BoxStream, Stream, StreamExt};
use kube_client::{api::ListParams, core::WatchEvent, Api, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

#[derive(Derivative)]
#[derivative(Debug)]
enum State<K: Resource + Clone> {
    Empty {
        init_resource_version: Option<String>,
    },
    InitListed {
        resource_version: String,
    },
    Watching {
        resource_version: String,
        #[derivative(Debug = "ignore")]
        stream: BoxStream<'static, kube_client::Result<WatchEvent<K>>>,
    },
}

async fn step_trampolined<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: &Api<K>,
    list_params: &ListParams,
    state: State<K>,
) -> (Option<Result<Event<K>>>, State<K>) {
    match state {
        State::Empty {
            init_resource_version,
        } => {
            let result = if let Some(init_resource_version) = init_resource_version {
                api.list_with_version(list_params, &init_resource_version).await
            } else {
                api.list(list_params).await
            };
            match result {
                Ok(list) => (Some(Ok(Event::Restarted(list.items))), State::InitListed {
                    resource_version: list.metadata.resource_version.unwrap(),
                }),
                Err(err) => {
                    let new_state = match &err {
                        kube_client::Error::Api(err) if err.code == 410 => State::Empty {
                            init_resource_version: None,
                        },
                        _ => State::Empty {
                            init_resource_version: Some("0".to_owned()),
                        },
                    };
                    (Some(Err(err).map_err(Error::InitialListFailed)), new_state)
                }
            }
        }
        State::InitListed { resource_version } => match api.watch(list_params, &resource_version).await {
            Ok(stream) => (None, State::Watching {
                resource_version,
                stream: stream.boxed(),
            }),
            Err(err) => (
                Some(Err(err).map_err(Error::WatchStartFailed)),
                State::InitListed { resource_version },
            ),
        },
        State::Watching {
            resource_version,
            mut stream,
        } => match stream.next().await {
            Some(Ok(WatchEvent::Added(obj) | WatchEvent::Modified(obj))) => {
                let resource_version = obj.resource_version().unwrap();
                (Some(Ok(Event::Applied(obj))), State::Watching {
                    resource_version,
                    stream,
                })
            }
            Some(Ok(WatchEvent::Deleted(obj))) => {
                let resource_version = obj.resource_version().unwrap();
                (Some(Ok(Event::Deleted(obj))), State::Watching {
                    resource_version,
                    stream,
                })
            }
            Some(Ok(WatchEvent::Bookmark(bm))) => (None, State::Watching {
                resource_version: bm.metadata.resource_version,
                stream,
            }),
            Some(Ok(WatchEvent::Error(err))) => {
                // HTTP GONE, means we have desynced and need to start over and re-list :(
                let new_state = if err.code == 410 {
                    State::Empty {
                        init_resource_version: Some("0".to_owned()),
                    }
                } else {
                    State::Watching {
                        resource_version,
                        stream,
                    }
                };
                (Some(Err(err).map_err(Error::WatchError)), new_state)
            }
            Some(Err(err)) => (Some(Err(err).map_err(Error::WatchFailed)), State::Watching {
                resource_version,
                stream,
            }),
            None => (None, State::InitListed { resource_version }),
        },
    }
}

async fn step<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: &Api<K>,
    list_params: &ListParams,
    mut state: State<K>,
) -> (Result<Event<K>>, State<K>) {
    loop {
        match step_trampolined(api, list_params, state).await {
            (Some(result), new_state) => return (result, new_state),
            (None, new_state) => state = new_state,
        }
    }
}

pub fn watcher<K: Resource + Clone + DeserializeOwned + Debug + Send + 'static>(
    api: Api<K>,
    list_params: ListParams,
) -> impl Stream<Item = Result<Event<K>>> + Send {
    futures::stream::unfold(
        (api, list_params, State::Empty {
            init_resource_version: Some("0".to_owned()),
        }),
        |(api, list_params, state)| async {
            let (event, state) = step(&api, &list_params, state).await;
            Some((event, (api, list_params, state)))
        },
    )
}
