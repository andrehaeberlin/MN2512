import os
import traceback
from datetime import timedelta
from typing import Any, Dict

from rq import Queue, get_current_job

from localDB import (
    STATUS_ERROR_PROCESSING,
    count_processing_docs,
    run_pipeline_for_document,
    try_acquire_processing_slot,
    update_document_status,
)

MAX_ACTIVE_DOCS = int(os.getenv("MAX_ACTIVE_DOCS", "2"))
REQUEUE_DELAY_S = int(os.getenv("REQUEUE_DELAY_S", "10"))


def process_document_job(document_id: str) -> Dict[str, Any]:
    try:
        if not try_acquire_processing_slot(document_id, max_active_docs=MAX_ACTIVE_DOCS):
            job = get_current_job()
            if job:
                q = Queue(job.origin, connection=job.connection)
                q.enqueue_in(timedelta(seconds=REQUEUE_DELAY_S), process_document_job, document_id, job_timeout=900)
            return {
                "ok": False,
                "requeued": True,
                "active_docs": count_processing_docs(),
                "message": f"Limite global atingido (MAX_ACTIVE_DOCS={MAX_ACTIVE_DOCS}).",
            }

        ok, message = run_pipeline_for_document(document_id)
        if not ok:
            update_document_status(document_id, STATUS_ERROR_PROCESSING, error_message=message)
            return {"ok": False, "error": message}

        return {"ok": True, "message": message}
    except Exception as exc:
        update_document_status(document_id, STATUS_ERROR_PROCESSING, error_message=str(exc))
        return {
            "ok": False,
            "error": str(exc),
            "trace": traceback.format_exc(),
        }
