use url::Url;

#[derive(Debug, Copy, Clone)]
pub enum Status {
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub struct Connection {
    url: Url,
    status: Status,
}

impl Connection {
    fn new(url: Url) -> Self {
        Self {
            url,
            status: Status::Disconnected,
        }
    }
}
