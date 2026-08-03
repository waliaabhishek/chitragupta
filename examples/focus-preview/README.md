# FOCUS Mapping Preview walkthrough

This example shows one complete path from Confluent Cloud source collection to a
downloaded FOCUS Mapping Preview package.

FOCUS Preview does not ingest a billing file supplied by the operator. The
ordinary Chitragupta pipeline gathers Confluent billing and inventory data,
calculates allocations, and persists the evidence. The package request in
[`request.json`](request.json) is the direct input to Preview.

## Files

- `config.yaml`: minimal local API, worker, tenant, and Preview configuration.
- `.env.example`: Confluent Cloud credentials used by `config.yaml`.
- `request.json`: one actual Daily Custom API request.
- `sample-output/`: representative output rendered by the current mapping and
  manifest code from one fictional Kafka storage charge.

The tenant's `focus_preview` block enables the feature. The top-level `preview`
block is optional because all process-wide settings have defaults. This example
sets only `artifact_root` to keep its generated packages separate from the
default `data/focus-preview` directory.

`effective_start_date` is required and included. This example omits
`effective_end_date`, so each ad-hoc request, scheduled cycle, or repair resolves
its exclusive end once from its own UTC creation or cycle date. Configure an
explicit end when a fixed commercial termination boundary is known; it remains
an exclusive hard override.

The resolved commercial interval does not widen `lookback_days`, `cutoff_days`,
or `retention_days`. The pipeline must still have complete persisted
calculation, source, allocation-lineage, reconciliation, and mapping evidence
for the requested interval.

The configured `billing_currency: USD` is normalized and copied into
`BillingCurrency` whenever the selected output includes that column. It is
authority for the eligible Direct-billed PAYG scope, not a value inferred from
individual Confluent Costs API records, which do not contain a per-record
currency field. Non-USD scopes remain fail-closed, and Preview performs no
currency conversion.

For the eligible Direct-billed PAYG scope, the mapping profile uses
`Confluent Cloud` as the participating entity for both `ServiceProviderName`
and `InvoiceIssuerName`. The issuer name is profile authority, not a per-record
field from the Confluent Costs API. Invoice-issuer-assigned `InvoiceId` and
`InvoiceDetailId` remain unavailable and null; Preview does not fabricate them
or perform invoice reconciliation.

The sample output is static and uses fictional IDs. A real run uses the billing
dates, organization, environments, resources, allocations, costs, and
quantities already persisted for your tenant.

## Run the example against your Confluent Cloud tenant

From the repository root:

```bash
cp examples/focus-preview/.env.example examples/focus-preview/.env
# Edit examples/focus-preview/.env with a Cloud API key that can read
# organization inventory and billing data.
mkdir -p data

uv run python src/main.py \
  --config-file examples/focus-preview/config.yaml \
  --validate

uv run python src/main.py \
  --config-file examples/focus-preview/config.yaml \
  --mode both
```

Wait for a successful pipeline cycle.

## Generate and export from the UI

Start the frontend in another terminal:

```bash
cd frontend
npm run dev
```

Open [http://localhost:5173/focus-preview](http://localhost:5173/focus-preview),
then:

1. Select tenant `ccloud-prod`.
2. Select **Daily**, start date `2026-07-01`, and end date `2026-07-02`.
3. Select **Custom** and choose the columns listed in `request.json`. Full and
   Summary are available when you do not need a custom projection.
4. Click **Generate preview**. The page submits and polls the request.
5. In **Recent requests**, download `manifest.json`, an individual CSV part,
   `focus-metadata.json`, or **Download All** as a ZIP.

The same page shows **Published monthly revisions**. Select a month, choose
**View and download**, then download its manifest, CSV parts, metadata, or
complete ZIP.

## Generate and export from the CLI

The CLI can submit, poll, verify, and download the sample package in one
command:

```bash
uv run chitragupta-preview request \
  --api-url http://localhost:8080/api/v1 \
  --tenant ccloud-prod \
  --start-date 2026-07-01 \
  --end-date 2026-07-02 \
  --column-profile custom \
  --column BillingAccountId \
  --column SubAccountId \
  --column ResourceId \
  --column ResourceName \
  --column ServiceName \
  --column ChargePeriodStart \
  --column ChargePeriodEnd \
  --column BilledCost \
  --column EffectiveCost \
  --column ConsumedQuantity \
  --column ConsumedUnit \
  --column AllocatedResourceId \
  --output-dir ./focus-preview-output
```

## Generate and export through the API

To submit the same input through the API instead, use `request.json`:

```bash
curl -fsS \
  -X POST \
  http://localhost:8080/api/v1/tenants/ccloud-prod/focus-preview/requests \
  -H 'Content-Type: application/json' \
  --data @examples/focus-preview/request.json
```

The API response is asynchronous. Copy its `request_id`, then poll and
download:

```bash
uv run chitragupta-preview status \
  --api-url http://localhost:8080/api/v1 \
  --tenant ccloud-prod \
  --wait \
  --json \
  REQUEST_ID

uv run chitragupta-preview download \
  --api-url http://localhost:8080/api/v1 \
  --tenant ccloud-prod \
  --output-dir ./focus-preview-output \
  REQUEST_ID
```

The requested interval must contain dates for which your pipeline has complete
retained Preview evidence and must end no later than the operation's exclusive
commercial end. Change the dates in `request.json` when the included July 2026
date is not applicable to your data.

## What the sample means

The fictional source charge is USD 8 of Kafka storage for organization `org-1`,
environment `env-1`, and cluster `lkc-1`. Chitragupta attributes the complete
charge and 5 GB quantity to service account `sa-1`.

[`sample-output/cost-and-usage.csv`](sample-output/cost-and-usage.csv) is the
selected FOCUS projection. [`sample-output/manifest.json`](sample-output/manifest.json)
records:

- target FOCUS version `1.4` and `non_conforming` Preview status;
- the exact selected columns and evidence interval;
- zero cost and quantity reconciliation differences;
- mapping and artifact-integrity validation;
- the CSV byte size and SHA-256 checksum;
- the current provider-authority gaps; `BillingCurrency` and
  `InvoiceIssuerName` use eligible-scope mapping authority and are not gaps; and
- the requested package's fixed seven-day download lifecycle.

[`sample-output/focus-metadata.json`](sample-output/focus-metadata.json) is the
exact canonical metadata artifact stored beside the CSV. It describes the data
generator, deterministic dataset/schema/recency identities, source freshness,
the exact ordered Custom columns, and the CSV relationship. Its
`x_ChitraguptaPreview...` sections are nonstandard import metadata for a
non-conforming Preview artifact, not official FOCUS Schema metadata and not a
FOCUS 1.4 conformance claim.

Importers should use the metadata schema and artifact links to interpret the
CSV, while treating `manifest.json` as the only authority for checksums, sizes,
expiry, revisions, known gaps, and lifecycle. The API, remote CLI, web UI,
individual-file download, and Download All return the same stored metadata
bytes. This requested sample declares `not_a_correction_series`; it is one
immutable requested snapshot. Published monthly revisions instead declare
Replacement correction handling with Overwrite delivery. Each published
revision is a complete current snapshot, so consumers replace the prior
revision and never aggregate revisions.

The manifest is formatted for readability here. Downloaded manifests use the
same JSON values in canonical compact form. The sample metadata file is already
in its exact canonical stored form, including its trailing newline.

For Monthly, Summary, Full, published-revision, repair, UI, and authentication
workflows, see [`docs/focus-mapping-preview.md`](../../docs/focus-mapping-preview.md).
