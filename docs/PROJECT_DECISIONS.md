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
