# Project Decisions

## 2026-07-13

### Architecture
- The system is a complete SFDA Drug Traceability & Reconciliation Platform.
- Azure Functions will remain the business logic layer.
- Azure SQL Database will be added as the primary database.
- Azure Blob Storage will store uploaded and generated files.
- Future integration with SAP / ERP must be supported.

### Master Data
- Generic Item Number is the primary identifier.
- Trade Item Number is linked to Generic Item Number.
- Product Intelligence will be implemented.
- Master Data will grow automatically after every reconciliation run.

### Reconciliation
- Every run must be stored permanently.
- Transaction Date and Reconciliation Date are separate.
- Historical data must support overlapping uploads without duplication.

### Web Application
- Upload & Run remain on one page.
- Results page becomes a searchable database.
- History page stores all reconciliation runs.
- Product Intelligence page is mandatory.
- Variance Management page is mandatory.

### Future
- SAP integration.
- ERP integration.
- Power BI integration.
- User roles and audit logging.

## 2026-09-02

### Return Reconciliation
- TRK49 (STO Return) and TRK30 (Customer Return) are stored together in the
  Historical Database `Returns History` sheet.
- Returns never produce Accept and never increase ordinary Dispatch history.
- ASN Supplier Name is the returning party and is matched to Customer History
  To Address; the approved warehouse GLN mapping supplies the regulatory GLN.
- Full Dispatch produces a separate Cancel Dispatch report and CSV group.
- Cancel quantity is capped by the original customer dispatch and previously
  confirmed cancellations are deducted before generating new quantities.
- Unmatched customer, missing GLN, and missing PackageSize rows are exceptions
  and cannot produce an automatic cancellation.
- Existing Accept and Full Dispatch calculation outputs remain unchanged.
