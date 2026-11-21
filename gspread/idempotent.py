"""
gspread.idempotent
~~~~~~~~~~~~~~~~~~

Helpers for exactly-once (idempotent) bulk append.

Every idempotent append writes two things in a single atomic
``spreadsheets.batchUpdate`` call:

* the rows of the batch (``AppendCellsRequest``), and
* a receipt (``CreateDeveloperMetadataRequest``) that binds the batch number
  to the target worksheet and to a digest of the appended content.

The receipt is stored as developer metadata with an explicit, deterministic
``metadataId``. The Sheets API rejects the whole batch if that ID already
exists, so two writers replaying the same batch number cannot both append:
exactly one of them wins and the other one observes the winner's receipt.
Receipts are never updated nor deleted; they stay in the document so that a
replay can always be verified.

Note that Google allows 30,000 characters of developer metadata per
spreadsheet, plus 30,000 for each sheet, and the receipt payload below is
deliberately compact to leave room for as many batches as possible.

This module is internal to :class:`gspread.worksheet.Worksheet`; the public
entry points are
:meth:`~gspread.worksheet.Worksheet.append_rows_idempotent`,
:meth:`~gspread.worksheet.Worksheet.append_row_idempotent`,
:meth:`~gspread.worksheet.Worksheet.batch_append` and
:meth:`~gspread.worksheet.Worksheet.get_append_receipt`.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .exceptions import BatchConflictError, BatchReceiptLookupError
from .utils import ValueInputOption

#: Key carried by every receipt. The key is shared by all batches; the
#: spreadsheet-scoped ``metadataId`` identifies a single batch.
RECEIPT_METADATA_KEY: str = "gspread.batch_append.receipt"

#: Version of the receipt payload, to allow a future migration.
RECEIPT_VERSION: int = 1

#: Base of the metadata ID namespace owned by gspread.
#: A batch number ``n`` maps to ``_METADATA_ID_NAMESPACE + n``.
_METADATA_ID_NAMESPACE: int = 1_000_000_000

#: Largest accepted batch number (the resulting ID must fit in an int32).
MAX_BATCH_ID: int = 2**31 - 1 - _METADATA_ID_NAMESPACE

#: Status of a call that appended the rows.
COMMITTED: str = "committed"

#: Status of a call that found a matching receipt from an earlier attempt.
ALREADY_COMMITTED: str = "already_committed"

_NUMERIC_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")

#: Receipt payload key -> :class:`AppendReceipt` key. Keys are kept short on
#: purpose: developer metadata is limited to 30,000 characters per document.
_PAYLOAD_FIELDS: Sequence[Tuple[str, str]] = (
    ("b", "batch_id"),
    ("p", "spreadsheet_id"),
    ("s", "sheet_id"),
    ("t", "sheet_title"),
    ("d", "content_digest"),
    ("r", "row_count"),
    ("c", "column_count"),
    ("a", "committed_at"),
)

DefaultSerializerType = Optional[Callable[[Any], Any]]


def validate_batch_id(batch_id: Any) -> int:
    """Validate and return a batch number.

    :param batch_id: a fixed positive integer identifying the batch.
    :raises TypeError: if ``batch_id`` is not an integer.
    :raises ValueError: if ``batch_id`` is not positive or is too large.
    """
    if isinstance(batch_id, bool) or not isinstance(batch_id, int):
        raise TypeError(
            "batch_id must be a positive integer, not {}".format(
                type(batch_id).__name__
            )
        )

    if batch_id <= 0:
        raise ValueError("batch_id must be a positive integer, got {}".format(batch_id))

    if batch_id > MAX_BATCH_ID:
        raise ValueError(
            "batch_id is too large (max {}), got {}".format(MAX_BATCH_ID, batch_id)
        )

    return batch_id


def metadata_id_for_batch_id(batch_id: int) -> int:
    """Return the spreadsheet-scoped developer metadata ID of a batch.

    The mapping is deterministic and injective: distinct batch numbers get
    distinct metadata IDs, and an ID is never reused for another batch.
    """
    return _METADATA_ID_NAMESPACE + validate_batch_id(batch_id)


def validate_values(values: Any) -> List[List[Any]]:
    """Normalize the rows of a batch to a list of lists.

    :raises TypeError: if ``values`` is not a sequence of rows.
    :raises ValueError: if there is no row at all.
    """
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError("values must be a sequence of rows, each a sequence of values")

    rows: List[List[Any]] = []
    for row in values:
        if isinstance(row, (str, bytes)) or not isinstance(row, Sequence):
            raise TypeError("each row must be a sequence of cell values")
        rows.append(list(row))

    if not rows:
        raise ValueError("values must contain at least one row")

    return rows


def _coerce(value: Any, default_serializer: DefaultSerializerType) -> Any:
    """Reduce a value to a JSON-native scalar usable by the Sheets API."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if default_serializer is None:
        raise TypeError(
            "cannot encode value of type {}; pass default_serializer "
            "(e.g. str) to append it".format(type(value).__name__)
        )

    converted = default_serializer(value)
    if converted is value or not isinstance(
        converted, (bool, int, float, str, type(None))
    ):
        raise TypeError(
            "default_serializer must convert {!r} to a JSON native value "
            "(str, int, float, bool or None), got {!r}".format(value, converted)
        )

    return converted


def _canonical(value: Any, default_serializer: DefaultSerializerType) -> Any:
    """Return a deterministic, JSON-friendly form of a cell value.

    Integral floats are folded into integers so that the same logical content
    (``1`` and ``1.0``, which Sheets stores identically) gets the same digest.
    """
    value = _coerce(value, default_serializer)

    if isinstance(value, float) and value.is_integer() and abs(value) < 2**53:
        return int(value)

    return value


def compute_content_digest(
    sheet_id: int,
    rows: Sequence[Sequence[Any]],
    value_input_option: Any,
    default_serializer: DefaultSerializerType = None,
) -> str:
    """Compute the digest that binds content and target worksheet.

    The digest covers the target sheet ID (not its title, which may change),
    the value input option and every cell of the batch. It is prefixed with
    ``sha256:``.

    The value input option is part of it because it changes what lands in the
    document: ``"123"`` is stored as text with ``RAW`` but as a number with
    ``USER_ENTERED``.
    """
    payload = {
        "v": RECEIPT_VERSION,
        "s": int(sheet_id),
        "i": str(value_input_option),
        "rows": [
            [_canonical(cell, default_serializer) for cell in row] for row in rows
        ],
    }

    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _parse_number(value: str) -> Any:
    if "." in value or "e" in value or "E" in value:
        return float(value)
    return int(value)


def _encode_cell(
    value: Any,
    user_entered: bool,
    default_serializer: DefaultSerializerType,
) -> Dict[str, Any]:
    """Encode a single cell as a ``CellData`` value."""
    value = _coerce(value, default_serializer)

    if value is None:
        return {}

    if isinstance(value, bool):
        return {"userEnteredValue": {"boolValue": value}}

    if isinstance(value, (int, float)):
        return {"userEnteredValue": {"numberValue": value}}

    # value is a string
    if user_entered:
        if value.startswith("'"):
            return {"userEnteredValue": {"stringValue": value[1:]}}
        if value.startswith("="):
            return {"userEnteredValue": {"formulaValue": value}}

        upper = value.upper()
        if upper in ("TRUE", "FALSE"):
            return {"userEnteredValue": {"boolValue": upper == "TRUE"}}

        if _NUMERIC_RE.match(value):
            return {"userEnteredValue": {"numberValue": _parse_number(value)}}

    return {"userEnteredValue": {"stringValue": value}}


def encode_rows(
    rows: Sequence[Sequence[Any]],
    value_input_option: Any,
    default_serializer: DefaultSerializerType = None,
) -> List[Dict[str, Any]]:
    """Encode rows as a list of ``RowData`` objects for ``AppendCellsRequest``."""
    user_entered = str(value_input_option) == str(ValueInputOption.user_entered)

    return [
        {
            "values": [
                _encode_cell(cell, user_entered, default_serializer) for cell in row
            ]
        }
        for row in rows
    ]


def build_receipt_payload(
    batch_id: int,
    spreadsheet_id: str,
    sheet_id: int,
    sheet_title: str,
    content_digest: str,
    row_count: int,
    column_count: int,
    committed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the payload stored as the receipt's ``metadataValue``."""
    return {
        "v": RECEIPT_VERSION,
        "b": batch_id,
        "p": spreadsheet_id,
        "s": int(sheet_id),
        "t": sheet_title,
        "d": content_digest,
        "r": row_count,
        "c": column_count,
        "a": committed_at
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }


def serialize_receipt_payload(payload: Mapping[str, Any]) -> str:
    """Serialize a receipt payload deterministically."""
    return json.dumps(
        dict(payload), ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )


class AppendReceipt(dict):
    """The receipt of a committed batch, as stored in the document.

    Behaves like a read-only mapping (it is a :class:`dict`). Keys are::

        batch_id, spreadsheet_id, sheet_id, sheet_title, content_digest,
        row_count, column_count, committed_at, version, metadata_id,
        metadata_key

    Keys may also be read as attributes: ``receipt.batch_id``.
    """

    def __init__(
        self, data: Mapping[str, Any], metadata_key: str = RECEIPT_METADATA_KEY
    ):
        super().__init__(data)
        self.metadata_key = metadata_key

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    @classmethod
    def from_metadata(
        cls, metadata: Mapping[str, Any], expected_batch_id: int
    ) -> "AppendReceipt":
        """Parse developer metadata returned by the API.

        :raises BatchReceiptLookupError: if the stored receipt cannot be
            verified (wrong key, malformed JSON, inconsistent fields). A
            receipt that cannot be verified is never interpreted.
        """
        if not isinstance(metadata, Mapping):
            raise BatchReceiptLookupError(
                "the developer metadata of batch {} is malformed and cannot "
                "be verified".format(expected_batch_id),
                batch_id=expected_batch_id,
            )

        metadata_key = metadata.get("metadataKey")
        if metadata_key != RECEIPT_METADATA_KEY:
            raise BatchReceiptLookupError(
                "metadata ID {} belongs to unrelated developer metadata "
                "(key {!r}), it is not a gspread batch receipt".format(
                    metadata.get("metadataId"), metadata_key
                ),
                batch_id=expected_batch_id,
            )

        payload = _decode_receipt_payload(
            metadata.get("metadataValue"), expected_batch_id
        )
        _check_receipt_header(payload, expected_batch_id)
        data = _extract_receipt_fields(payload, expected_batch_id)

        metadata_id = metadata.get("metadataId")
        if metadata_id != metadata_id_for_batch_id(expected_batch_id):
            raise BatchReceiptLookupError(
                "the receipt of batch {} is stored under unexpected metadata "
                "ID {!r}".format(expected_batch_id, metadata_id),
                batch_id=expected_batch_id,
            )

        data["version"] = payload["v"]
        data["metadata_id"] = metadata_id
        data["metadata_key"] = metadata_key

        return cls(data, metadata_key=metadata_key)


def _decode_receipt_payload(raw_value: Any, batch_id: int) -> Dict[str, Any]:
    """Decode the receipt JSON carried by the metadata value."""
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError) as err:
        raise BatchReceiptLookupError(
            "the receipt of batch {} is not valid JSON and cannot be "
            "verified".format(batch_id),
            batch_id=batch_id,
        ) from err

    if not isinstance(payload, dict):
        raise BatchReceiptLookupError(
            "the receipt of batch {} is malformed and cannot be "
            "verified".format(batch_id),
            batch_id=batch_id,
        )

    return payload


def _check_receipt_header(payload: Mapping[str, Any], batch_id: int) -> None:
    """Check the version and the batch number claimed by a receipt."""
    if payload.get("v") != RECEIPT_VERSION:
        raise BatchReceiptLookupError(
            "the receipt of batch {} has unsupported version {!r}".format(
                batch_id, payload.get("v")
            ),
            batch_id=batch_id,
        )

    if payload.get("b") != batch_id:
        raise BatchReceiptLookupError(
            "the receipt stored for batch {} claims batch {!r}; it cannot "
            "be verified".format(batch_id, payload.get("b")),
            batch_id=batch_id,
        )


def _extract_receipt_fields(
    payload: Mapping[str, Any], batch_id: int
) -> Dict[str, Any]:
    """Copy the receipt fields, checking that each one is present and sane."""
    data: Dict[str, Any] = {}

    for payload_key, name in _PAYLOAD_FIELDS:
        if payload_key not in payload:
            raise BatchReceiptLookupError(
                "the receipt of batch {} is missing {!r} and cannot be "
                "verified".format(batch_id, name),
                batch_id=batch_id,
            )
        data[name] = payload[payload_key]

    digest = data["content_digest"]
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise BatchReceiptLookupError(
            "the receipt of batch {} has an invalid digest".format(batch_id),
            batch_id=batch_id,
        )

    return data


class BatchAppendResult(dict):
    """The outcome of :meth:`Worksheet.append_rows_idempotent`.

    Behaves like a mapping with the following keys (also readable as
    attributes)::

        status             "committed" or "already_committed"
        already_committed  True when the batch had been committed earlier
        batch_id           the batch number
        content_digest     digest of the content and target worksheet
        metadata_id        developer metadata ID holding the receipt
        spreadsheet_id     the document the batch belongs to
        sheet_id           the worksheet the batch was appended to
        sheet_title        title of that worksheet at commit time
        row_count          rows appended by this call (0 when replayed)
        receipt            the :class:`AppendReceipt`
        response           raw batchUpdate response (None when replayed)
    """

    def __init__(
        self,
        status: str,
        receipt: AppendReceipt,
        row_count: int,
        response: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(
            status=status,
            already_committed=status == ALREADY_COMMITTED,
            batch_id=receipt["batch_id"],
            content_digest=receipt["content_digest"],
            metadata_id=receipt["metadata_id"],
            spreadsheet_id=receipt["spreadsheet_id"],
            sheet_id=receipt["sheet_id"],
            sheet_title=receipt["sheet_title"],
            row_count=row_count,
            receipt=receipt,
            response=response,
        )

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    @property
    def committed(self) -> bool:
        """True when this call appended the rows."""
        return self["status"] == COMMITTED


def check_receipt_matches(
    receipt: AppendReceipt,
    batch_id: int,
    spreadsheet_id: str,
    sheet_id: int,
    content_digest: str,
) -> None:
    """Verify that an existing receipt describes the requested batch.

    :raises BatchConflictError: if the batch number was already used with a
        different target worksheet or different content.
    :raises BatchReceiptLookupError: if the receipt refers to another document.
    """
    if receipt["spreadsheet_id"] != spreadsheet_id:
        raise BatchReceiptLookupError(
            "the receipt of batch {} belongs to another spreadsheet".format(batch_id),
            batch_id=batch_id,
        )

    if receipt["sheet_id"] != sheet_id:
        raise BatchConflictError(
            "batch {} was already committed to worksheet {!r} (sheet ID {}); "
            "it cannot be replayed against sheet ID {}".format(
                batch_id,
                receipt["sheet_title"],
                receipt["sheet_id"],
                sheet_id,
            ),
            batch_id=batch_id,
            existing=receipt,
        )

    if receipt["content_digest"] != content_digest:
        raise BatchConflictError(
            "batch {} was already committed with different content "
            "(receipt digest {}, requested digest {}); use a new batch "
            "number".format(batch_id, receipt["content_digest"], content_digest),
            batch_id=batch_id,
            existing=receipt,
        )
