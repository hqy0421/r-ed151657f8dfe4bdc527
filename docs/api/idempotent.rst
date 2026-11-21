Idempotent append
=================

Helpers used by :meth:`~gspread.worksheet.Worksheet.append_rows_idempotent`
and friends. See :ref:`the user guide <appending-rows-exactly-once>` for the
protocol.

.. autoclass:: gspread.idempotent.AppendReceipt
   :members:

.. autoclass:: gspread.idempotent.BatchAppendResult
   :members:

.. autofunction:: gspread.idempotent.validate_batch_id

.. autofunction:: gspread.idempotent.metadata_id_for_batch_id

.. autofunction:: gspread.idempotent.compute_content_digest

.. autofunction:: gspread.idempotent.encode_rows

.. autodata:: gspread.idempotent.RECEIPT_METADATA_KEY

.. autodata:: gspread.idempotent.MAX_BATCH_ID

.. autodata:: gspread.idempotent.COMMITTED

.. autodata:: gspread.idempotent.ALREADY_COMMITTED
