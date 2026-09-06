# Complaint-theme labelling runbook

This runbook fixes what leaves the machine. Bulk frozen work uses Anthropic Message Batches;
prompt development uses the standard Messages API. The demo uses cached Iceberg rows only.

## Exact request body

Freeze the model id, system prompt, taxonomy, schema hash and inference settings in the protocol
commit. For each review, submit this JSON body (Batch wraps it in its required `custom_id` and
`params`):

```json
{
  "model": "claude-haiku-4-5-20251001",
  "max_tokens": 512,
  "temperature": 0,
  "system": "<verbatim frozen complaint-theme prompt, taxonomy, and output rules>",
  "messages": [
    {
      "role": "user",
      "content": [
        {
          "type": "text",
          "text": "{\"title\":<JSON-encoded title or null>,\"text\":<JSON-encoded review text or null>}"
        }
      ]
    }
  ]
}
```

The embedded review object has exactly two keys: `title` and `text`. Construct it with a JSON
serializer, never string interpolation. Do not add rating, `user_id`, ASIN/parent ASIN, timestamp,
product title/name, metadata, split, human labels, or source id. For Batch, generate a fresh random
UUID as `custom_id` for each provider attempt and keep its mapping to the local idempotency key only
in Iceberg. It must not be derived from a review or source identifier.

## Before a hosted run

1. Confirm the taxonomy, prompt, schema and inference config are committed and record their
   hashes. The audit run additionally requires the frozen commit id.
2. Confirm the selected source ids are disjoint across discovery, development, training and
   audit. Holdout inference may overlap audit only for evaluation and must reuse the cached
   logical inference rather than call twice.
3. Confirm the Claude Console workspace limit is $20 and the local ledger is below both 12,800
   logical calls and $20 estimated cumulative spend.
4. Render a payload sample and assert its embedded review object has exactly `title` and `text`.
5. Submit Batch work; poll and cache every raw response. Never make a hosted call during the demo.

## Validation and retries

Apply `conf/complaint-theme-label.schema.json` plus the semantic checks in ADR-0003. Retry an API
or validation failure once with identical inputs. Store both attempts. After the second failure,
write terminal `parse_failed` or `api_failed` status and no label. Model abstention is a valid,
separately reported output.
