"""Client for the llm-d-async processor's HTTP API: submit requests and
read their results, synchronously (`Client`) or with asyncio
(`AsyncClient`)."""

from llm_d_async._errors import LlmDAsyncError, NotFoundError, OwnershipLostError, StatusError
from llm_d_async._types import Delivery, ErrorCode, Result, Submission, Submitted
from llm_d_async.aio import AsyncClient, AsyncResultBody
from llm_d_async.client import Client, ResultBody

__all__ = [
    "AsyncClient",
    "AsyncResultBody",
    "Client",
    "Delivery",
    "ErrorCode",
    "LlmDAsyncError",
    "NotFoundError",
    "OwnershipLostError",
    "Result",
    "ResultBody",
    "StatusError",
    "Submission",
    "Submitted",
]
