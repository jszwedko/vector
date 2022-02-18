// ## skip check-events ##

use metrics::counter;
use uuid::Uuid;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub(crate) struct AzureBlobErrorResponse {
    pub(crate) code: hyper::StatusCode,
}

impl InternalEvent for AzureBlobErrorResponse {
    fn emit_logs(&self) {
        error!(message = "HTTP error response.", code = %self.code);
    }

    fn emit_metrics(&self) {
        counter!("http_error_response_total", 1);
    }
}

#[derive(Debug)]
pub(crate) struct AzureBlobHttpError {
    pub error: String,
}

impl InternalEvent for AzureBlobHttpError {
    fn emit_logs(&self) {
        error!(
            message = "Error processing request.",
            error = %self.error,
            internal_log_rate_secs = 10
        );
    }

    fn emit_metrics(&self) {
        counter!("http_request_errors_total", 1);
    }
}

pub struct AzureBlobEventSent {
    pub(crate) request_id: Uuid,
    pub byte_size: usize,
}

impl InternalEvent for AzureBlobEventSent {
    fn emit_logs(&self) {
        trace!(message = "Event sent.", request_id = %self.request_id);
    }

    fn emit_metrics(&self) {
        counter!("processed_bytes_total", self.byte_size as u64);
    }
}
