import json
from io import BytesIO
from typing import Any, Dict, List, Mapping, Optional, cast
from unittest import TestCase
from unittest.mock import Mock

from requests import Response
from requests.exceptions import ReadTimeout

from gspread.exceptions import APIError, BatchConflictError, BatchReceiptLookupError
from gspread.http_client import HTTPClient
from gspread.idempotent import (
    ALREADY_COMMITTED,
    COMMITTED,
    MAX_BATCH_ID,
    RECEIPT_METADATA_KEY,
    AppendReceipt,
    BatchAppendResult,
    compute_content_digest,
    encode_rows,
    metadata_id_for_batch_id,
    validate_batch_id,
)
from gspread.utils import ValueInputOption
from gspread.worksheet import Worksheet


def make_api_error(code: int, message: str) -> APIError:
    response = Response()
    response.status_code = code
    response.raw = BytesIO(
        json.dumps({"error": {"code": code, "message": message}}).encode("utf-8")
    )
    return APIError(response)


class FakeSheets(HTTPClient):
    """A minimal in-memory model of the two API calls used by an idempotent
    append: ``batchUpdate`` (atomic) and ``developerMetadata.get``."""

    def __init__(self) -> None:
        super().__init__(auth=None, session=Mock())
        self.rows: List[List[Any]] = []
        self.metadata: Dict[int, Dict[str, Any]] = {}
        self.batch_update_calls = 0
        self.get_calls = 0

        # Behaviour injected by a test:
        self.before_batch_update = None  # callable(fake) run before applying
        self.after_batch_update = None  # callable(fake) run after applying
        self.fail_before_apply = None  # exception raised before applying
        self.get_error: Optional[Exception] = None

    # HTTPClient surface used by Worksheet
    def batch_update(
        self, spreadsheet_id: str, body: Optional[Mapping[str, Any]]
    ) -> Any:
        if body is None:  # pragma: no cover
            body = {}
        self.batch_update_calls += 1

        if self.before_batch_update is not None:
            self.before_batch_update(self)

        if self.fail_before_apply is not None:
            error, self.fail_before_apply = self.fail_before_apply, None
            raise error

        self._validate(body["requests"])
        replies = self._apply(body["requests"])

        response = {"spreadsheetId": spreadsheet_id, "replies": replies}

        if self.after_batch_update is not None:
            self.after_batch_update(self)

        return response

    def _validate(self, requests: List[Dict[str, Any]]) -> None:
        """Reject the whole batch if a metadata ID already exists."""
        for request in requests:
            if "createDeveloperMetadata" not in request:
                continue
            metadata_id = request["createDeveloperMetadata"]["developerMetadata"][
                "metadataId"
            ]
            if metadata_id in self.metadata:
                raise make_api_error(
                    400,
                    "Invalid requests[1].createDeveloperMetadata: "
                    'Developer metadata with ID "{}" already exists.'.format(
                        metadata_id
                    ),
                )

    def _apply(self, requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        replies: List[Dict[str, Any]] = []
        for request in requests:
            if "appendCells" in request:
                appended = request["appendCells"]["rows"]
                self.rows.extend(
                    [
                        [self._cell_to_value(cell) for cell in row["values"]]
                        for row in appended
                    ]
                )
                replies.append({})
            elif "createDeveloperMetadata" in request:
                metadata = request["createDeveloperMetadata"]["developerMetadata"]
                self.metadata[metadata["metadataId"]] = dict(metadata)
                replies.append(
                    {"createDeveloperMetadata": {"developerMetadata": metadata}}
                )
            else:  # pragma: no cover
                raise AssertionError("unexpected request: {}".format(request))
        return replies

    def get_developer_metadata(self, spreadsheet_id: str, metadata_id: int) -> Any:
        self.get_calls += 1
        if self.get_error is not None:
            raise self.get_error
        try:
            return self.metadata[metadata_id]
        except KeyError:
            raise make_api_error(404, "Requested entity was not found.") from None

    @staticmethod
    def _cell_to_value(cell: Dict[str, Any]) -> Any:
        value = cell.get("userEnteredValue")
        if value is None:
            return None
        for key in ("stringValue", "numberValue", "boolValue", "formulaValue"):
            if key in value:
                return value[key]
        raise AssertionError("unexpected cell: {}".format(cell))


class WorksheetForAppend:
    def __init__(self, fake: FakeSheets, sheet_id: int = 0, title: str = "Sheet1"):
        self.client = fake
        self.spreadsheet_id = "spreadsheet-id"
        self.worksheet = Worksheet(
            spreadsheet=cast(Any, None),
            properties={"sheetId": sheet_id, "title": title},
            spreadsheet_id="spreadsheet-id",
            client=fake,
        )


class BatchIdValidationTest(TestCase):
    def test_rejects_non_integers(self):
        for bad in ("1", 1.0, None, True, [1]):
            with self.assertRaises(TypeError):
                validate_batch_id(bad)

    def test_rejects_non_positive(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                validate_batch_id(bad)

    def test_rejects_too_large(self):
        with self.assertRaises(ValueError):
            validate_batch_id(MAX_BATCH_ID + 1)

    def test_accepts_max(self):
        self.assertEqual(validate_batch_id(MAX_BATCH_ID), MAX_BATCH_ID)

    def test_metadata_id_is_injective_and_int32(self):
        ids = {metadata_id_for_batch_id(n) for n in (1, 2, 3, MAX_BATCH_ID)}
        self.assertEqual(len(ids), 4)
        self.assertTrue(all(0 < i < 2**31 for i in ids))


class ContentDigestTest(TestCase):
    def test_stable_for_equal_content(self):
        a = compute_content_digest(1, [["x", 1]], "RAW")
        b = compute_content_digest(1, [["x", 1]], "RAW")
        self.assertEqual(a, b)
        self.assertTrue(a.startswith("sha256:"))

    def test_integral_floats_match_integers(self):
        self.assertEqual(
            compute_content_digest(1, [[1]], "RAW"),
            compute_content_digest(1, [[1.0]], "RAW"),
        )

    def test_binds_target_and_content(self):
        base = compute_content_digest(1, [["x"]], "RAW")
        self.assertNotEqual(base, compute_content_digest(2, [["x"]], "RAW"))
        self.assertNotEqual(base, compute_content_digest(1, [["y"]], "RAW"))
        self.assertNotEqual(base, compute_content_digest(1, [["x"]], "USER_ENTERED"))
        self.assertNotEqual(base, compute_content_digest(1, [["x"], ["y"]], "RAW"))

    def test_default_serializer_is_applied(self):
        import datetime

        digest = compute_content_digest(
            1, [[datetime.date(2026, 1, 2)]], "RAW", default_serializer=str
        )
        self.assertEqual(
            digest,
            compute_content_digest(1, [["2026-01-02"]], "RAW"),
        )


class EncodeRowsTest(TestCase):
    def test_raw_strings_stay_strings(self):
        self.assertEqual(
            encode_rows([["123", "=A1", "TRUE"]], ValueInputOption.raw),
            [
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": "123"}},
                        {"userEnteredValue": {"stringValue": "=A1"}},
                        {"userEnteredValue": {"stringValue": "TRUE"}},
                    ]
                }
            ],
        )

    def test_user_entered_parses_values(self):
        self.assertEqual(
            encode_rows([["123", "1.5", "=A1", "TRUE", "'007"]], "USER_ENTERED"),
            [
                {
                    "values": [
                        {"userEnteredValue": {"numberValue": 123}},
                        {"userEnteredValue": {"numberValue": 1.5}},
                        {"userEnteredValue": {"formulaValue": "=A1"}},
                        {"userEnteredValue": {"boolValue": True}},
                        {"userEnteredValue": {"stringValue": "007"}},
                    ]
                }
            ],
        )

    def test_empty_values_are_blank_cells(self):
        self.assertEqual(
            encode_rows([[None, ""]], "RAW"),
            [
                {
                    "values": [
                        {},
                        {"userEnteredValue": {"stringValue": ""}},
                    ]
                }
            ],
        )

    def test_non_serializable_value_requires_serializer(self):
        import datetime

        with self.assertRaises(TypeError):
            encode_rows([[datetime.date(2026, 1, 2)]], "RAW")

        self.assertEqual(
            encode_rows([[datetime.date(2026, 1, 2)]], "RAW", default_serializer=str),
            [{"values": [{"userEnteredValue": {"stringValue": "2026-01-02"}}]}],
        )


class AppendRowsIdempotentTest(TestCase):
    def setUp(self):
        self.fake = FakeSheets()
        self.worksheet = WorksheetForAppend(self.fake).worksheet

    def test_first_call_appends_once_and_writes_receipt(self):
        result = self.worksheet.append_rows_idempotent(7, [["a", 1], ["b", 2]])

        self.assertEqual(result["status"], COMMITTED)
        self.assertIsInstance(result, BatchAppendResult)
        self.assertEqual(result.batch_id, 7)
        self.assertEqual(result.row_count, 2)
        self.assertEqual(self.fake.rows, [["a", 1], ["b", 2]])
        self.assertEqual(self.fake.batch_update_calls, 1)

        receipt = self.fake.metadata[metadata_id_for_batch_id(7)]
        self.assertEqual(receipt["metadataKey"], RECEIPT_METADATA_KEY)
        self.assertEqual(receipt["location"], {"sheetId": 0})
        self.assertEqual(
            result["content_digest"],
            compute_content_digest(0, [["a", 1], ["b", 2]], "RAW"),
        )

    def test_replay_same_batch_does_not_append_again(self):
        first = self.worksheet.append_rows_idempotent(7, [["a", 1]])
        replay = self.worksheet.append_rows_idempotent(7, [["a", 1]])

        self.assertEqual(replay["status"], ALREADY_COMMITTED)
        self.assertTrue(replay.already_committed)
        self.assertFalse(replay.committed)
        self.assertEqual(replay.row_count, 0)
        self.assertEqual(first.content_digest, replay.content_digest)
        self.assertEqual(self.fake.rows, [["a", 1]])
        self.assertEqual(self.fake.batch_update_calls, 1)

    def test_replay_with_other_content_conflicts(self):
        self.worksheet.append_rows_idempotent(7, [["a", 1]])

        with self.assertRaises(BatchConflictError) as raised:
            self.worksheet.append_rows_idempotent(7, [["a", 2]])

        self.assertEqual(raised.exception.batch_id, 7)
        self.assertEqual(raised.exception.existing["batch_id"], 7)
        self.assertEqual(self.fake.rows, [["a", 1]])

    def test_replay_on_other_sheet_conflicts(self):
        self.worksheet.append_rows_idempotent(7, [["a", 1]])

        other = WorksheetForAppend(self.fake, sheet_id=42, title="Other").worksheet

        with self.assertRaises(BatchConflictError) as raised:
            other.append_rows_idempotent(7, [["a", 1]])

        self.assertEqual(raised.exception.batch_id, 7)
        self.assertEqual(self.fake.rows, [["a", 1]])

    def test_distinct_batches_both_append(self):
        self.worksheet.append_rows_idempotent(1, [["a"]])
        self.worksheet.append_rows_idempotent(2, [["b"]])

        self.assertEqual(self.fake.rows, [["a"], ["b"]])

    def test_row_wrapper_and_alias(self):
        result = self.worksheet.append_row_idempotent(3, ["a", 1])
        self.assertEqual(result.row_count, 1)

        replay = self.worksheet.batch_append(3, [["a", 1]])
        self.assertEqual(replay["status"], ALREADY_COMMITTED)

    def test_get_append_receipt(self):
        self.assertIsNone(self.worksheet.get_append_receipt(9))

        self.worksheet.append_rows_idempotent(9, [["a"]])

        receipt = self.worksheet.get_append_receipt(9)
        self.assertIsInstance(receipt, AppendReceipt)
        self.assertEqual(receipt.batch_id, 9)
        self.assertEqual(receipt.sheet_title, "Sheet1")
        self.assertEqual(receipt.row_count, 1)

    def test_validation_errors(self):
        with self.assertRaises(TypeError):
            self.worksheet.append_rows_idempotent("7", [["a"]])
        with self.assertRaises(ValueError):
            self.worksheet.append_rows_idempotent(0, [["a"]])
        with self.assertRaises(ValueError):
            self.worksheet.append_rows_idempotent(7, [])
        with self.assertRaises(TypeError):
            self.worksheet.append_rows_idempotent(7, "not rows")
        with self.assertRaises(ValueError):
            self.worksheet.append_rows_idempotent(7, [["a"]], max_attempts=0)
        with self.assertRaises(TypeError):
            self.worksheet.append_rows_idempotent(7, [["a"]], max_attempts=True)

    def test_metadata_id_collision_with_matching_receipt_is_replay(self):
        # Another writer committed this very batch between our lookup and
        # our batchUpdate: the API rejects the whole batch and we observe the
        # winner's receipt.
        def other_writer(fake):
            fake.before_batch_update = None
            WorksheetForAppend(fake).worksheet.append_rows_idempotent(7, [["a", 1]])

        self.fake.before_batch_update = other_writer

        result = self.worksheet.append_rows_idempotent(7, [["a", 1]])

        self.assertEqual(result["status"], ALREADY_COMMITTED)
        self.assertEqual(self.fake.rows, [["a", 1]])

    def test_metadata_id_collision_with_other_content_conflicts(self):
        def other_writer(fake):
            fake.before_batch_update = None
            WorksheetForAppend(fake).worksheet.append_rows_idempotent(7, [["other"]])

        self.fake.before_batch_update = other_writer

        with self.assertRaises(BatchConflictError):
            self.worksheet.append_rows_idempotent(7, [["a", 1]])

        self.assertEqual(self.fake.rows, [["other"]])

    def test_timeout_after_commit_is_verified_by_lookup(self):
        def apply_then_timeout(fake):
            raise ReadTimeout("read timed out")

        self.fake.after_batch_update = apply_then_timeout

        result = self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertEqual(result["status"], COMMITTED)
        self.assertEqual(self.fake.rows, [["a"]])
        # lookup before the write, then the lookup that verifies it
        self.assertEqual(self.fake.get_calls, 2)

    def test_timeout_before_commit_is_retried(self):
        self.fake.fail_before_apply = ReadTimeout("read timed out")

        result = self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertEqual(result["status"], COMMITTED)
        self.assertEqual(self.fake.batch_update_calls, 2)
        self.assertEqual(self.fake.rows, [["a"]])

    def test_server_error_after_commit_is_verified_by_lookup(self):
        def fail_after(fake):
            raise make_api_error(503, "The service is currently unavailable.")

        self.fake.after_batch_update = fail_after

        result = self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertEqual(result["status"], COMMITTED)
        self.assertEqual(self.fake.rows, [["a"]])

    def test_rejected_batch_raises_without_guessing(self):
        self.fake.fail_before_apply = make_api_error(
            400, "Invalid requests[0].appendCells: bad value"
        )

        with self.assertRaises(APIError):
            self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertEqual(self.fake.rows, [])
        self.assertEqual(self.fake.batch_update_calls, 1)

    def test_unverifiable_outcome_raises_instead_of_guessing(self):
        # The write goes through, but the receipt can no longer be read.
        def fail_after(fake):
            raise ReadTimeout("read timed out")

        self.fake.after_batch_update = fail_after
        self.fake.get_error = make_api_error(503, "backend error")

        with self.assertRaises(BatchReceiptLookupError):
            self.worksheet.append_rows_idempotent(7, [["a"]])

    def test_lookup_failure_before_write_is_not_guessed(self):
        self.fake.get_error = ReadTimeout("read timed out")

        with self.assertRaises(BatchReceiptLookupError):
            self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertEqual(self.fake.rows, [])
        self.assertEqual(self.fake.batch_update_calls, 0)

    def test_unknown_outcome_after_max_attempts(self):
        self.fake.fail_before_apply = make_api_error(500, "backend error")
        self.fake.get_error = make_api_error(500, "backend error")

        with self.assertRaises(BatchReceiptLookupError) as raised:
            self.worksheet.append_rows_idempotent(7, [["a"]], max_attempts=2)

        self.assertEqual(raised.exception.batch_id, 7)
        self.assertEqual(self.fake.rows, [])

    def test_response_without_receipt_is_verified_remotely(self):
        # A successful response that does not echo the receipt must not be
        # trusted on its own: the receipt is fetched and checked.
        worksheet = self.worksheet
        original = self.fake.batch_update

        def batch_update_without_receipt(spreadsheet_id, body):
            response = original(spreadsheet_id, body)
            response["replies"] = [{}, {}]
            return response

        self.fake.batch_update = batch_update_without_receipt
        try:
            result = worksheet.append_rows_idempotent(7, [["a"]])
        finally:
            self.fake.batch_update = original

        self.assertEqual(result["status"], COMMITTED)
        self.assertEqual(result.row_count, 1)
        self.assertEqual(self.fake.rows, [["a"]])
        # one lookup before the write, one to verify the write
        self.assertEqual(self.fake.get_calls, 2)

    def test_success_without_remote_receipt_is_an_error(self):
        original = self.fake.batch_update

        def batch_update_without_receipt(spreadsheet_id, body):
            response = original(spreadsheet_id, body)
            # Drop the receipt from the document as well as from the reply.
            self.fake.metadata.clear()
            response["replies"] = [{}, {}]
            return response

        self.fake.batch_update = batch_update_without_receipt
        try:
            with self.assertRaises(BatchReceiptLookupError):
                self.worksheet.append_rows_idempotent(7, [["a"]])
        finally:
            self.fake.batch_update = original

    def test_tampered_receipt_is_not_trusted(self):
        self.worksheet.append_rows_idempotent(7, [["a"]])
        metadata = self.fake.metadata[metadata_id_for_batch_id(7)]
        payload = json.loads(metadata["metadataValue"])
        payload["b"] = 8
        metadata["metadataValue"] = json.dumps(payload)

        with self.assertRaises(BatchReceiptLookupError):
            self.worksheet.append_rows_idempotent(7, [["a"]])

    def test_unrelated_metadata_is_not_trusted(self):
        metadata = {
            "metadataId": metadata_id_for_batch_id(7),
            "metadataKey": "someone.else",
            "metadataValue": "not a receipt",
        }
        self.fake.metadata[metadata_id_for_batch_id(7)] = metadata

        with self.assertRaises(BatchReceiptLookupError):
            self.worksheet.get_append_receipt(7)

    def test_receipt_lookup_uses_get_by_metadata_id(self):
        self.worksheet.append_rows_idempotent(7, [["a"]])

        self.assertIn(metadata_id_for_batch_id(7), self.fake.metadata)


class ConcurrentWritersTest(TestCase):
    """Two writers replaying the same batch number append exactly one copy."""

    def test_only_one_copy_is_appended(self):
        fake = FakeSheets()
        first = WorksheetForAppend(fake).worksheet
        second = WorksheetForAppend(fake).worksheet

        results = []

        def writer(worksheet):
            results.append(worksheet.append_rows_idempotent(5, [["row"]]))

        writer(first)
        writer(second)

        self.assertEqual(fake.rows, [["row"]])
        self.assertEqual(
            sorted(r["status"] for r in results), [ALREADY_COMMITTED, COMMITTED]
        )
        self.assertEqual(fake.batch_update_calls, 1)

    def test_interleaved_writers_do_not_double_append(self):
        fake = FakeSheets()
        first = WorksheetForAppend(fake).worksheet
        second = WorksheetForAppend(fake).worksheet

        # The second writer got past its receipt lookup before the first one
        # wrote; only the atomic metadata-ID collision can stop it.
        def interleave(_fake):
            _fake.before_batch_update = None
            first.append_rows_idempotent(5, [["row"]])

        fake.before_batch_update = interleave

        result = second.append_rows_idempotent(5, [["row"]])

        self.assertEqual(result["status"], ALREADY_COMMITTED)
        self.assertEqual(fake.rows, [["row"]])
        self.assertEqual(fake.batch_update_calls, 2)
