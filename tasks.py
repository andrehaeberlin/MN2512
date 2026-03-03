import traceback
from typing import Any, Dict

from localDB import (
    STATUS_ERROR_PROCESSING,
    STATUS_HITL_REVIEW,
    STATUS_PROCESSING,
    run_pipeline_for_document,
    update_document_status,
)


def process_document_job(document_id: str) -> Dict[str, Any]:
    try:
        update_document_status(document_id, STATUS_PROCESSING)
        ok, message = run_pipeline_for_document(document_id)
        if not ok:
            update_document_status(document_id, STATUS_ERROR_PROCESSING, error_message=message)
            return {"ok": False, "error": message}

        update_document_status(document_id, STATUS_HITL_REVIEW)
        return {"ok": True, "message": message}
    except Exception as exc:
        update_document_status(document_id, STATUS_ERROR_PROCESSING, error_message=str(exc))
        return {
            "ok": False,
            "error": str(exc),
            "trace": traceback.format_exc(),
        }
