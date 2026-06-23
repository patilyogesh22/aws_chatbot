DELETE FROM structured_datasets a
USING structured_datasets b
WHERE a.id > b.id
  AND a.user_id = b.user_id
  AND a.document_id = b.document_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_structured_datasets_user_document
ON structured_datasets(user_id, document_id);