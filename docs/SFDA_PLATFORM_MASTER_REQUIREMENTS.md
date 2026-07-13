# SFDA Drug Traceability & Reconciliation Platform
## Master Requirements, Business Logic, Data Model, and Roadmap

**Project Owner:** Safwan Noor  
**Business Area:** Healthcare Supply Chain / Madinah Logistics Center  
**Recommended GitHub path:** `docs/SFDA_PLATFORM_MASTER_REQUIREMENTS.md`

---

# 1. Project Vision

The final solution is not only a file-upload and reconciliation tool. It is a complete **SFDA Drug Traceability & Reconciliation Platform**.

The system must:

- Reconcile WMS receiving, inventory, and dispatch data against SFDA Drug Count data.
- Generate SFDA Accept and Dispatch upload files.
- Store every reconciliation run permanently.
- Store detailed receiving, dispatch, inventory, SFDA, Accept, Dispatch, and Variance records.
- Allow historical search by date, batch, product, supplier, customer, run, and status.
- Track the complete journey of each drug batch from supplier receipt to customer dispatch.
- Build an evolving Product Intelligence knowledge base.
- Detect human drugs that should be registered in SFDA but are missing from the current SFDA report.
- Support future integration with SAP, ERP, WMS, Power BI, and other enterprise systems.

---

# 2. Current Working System

Current components:

- Azure Functions backend.
- Python reconciliation engine.
- Azure-hosted web interface.
- Four uploaded Excel files:
  - ASN Receipt Detailed Report.
  - Inventory Report.
  - Full Dispatch Report.
  - SFDA Drug Count.
- Internal reference files:
  - Pack Size mapping.
  - GLN customer mapping.
- Current outputs:
  - SFDA Accept CSV files.
  - SFDA Dispatch CSV files grouped by customer.
  - Accept Details Excel.
  - Dispatch Details Excel.
  - Variance Report Excel.

Current API:

```text
POST /api/process
```

Required multipart fields:

```text
asn
inventory
dispatch
sfda
```

The current backend is stateless. Persistent database and file storage are required in the next phase.

---

# 3. Core Product Definitions

## 3.1 Generic Item Number

The **Generic Item Number** represents the main pharmaceutical item or generic drug category.

Example:

| Generic Item Number | Generic Drug |
|---|---|
| 1000 | Paracetamol |

The Generic Item Number is the primary indicator for identifying whether an item belongs to the SFDA human-drug tracking scope.

## 3.2 Trade Item Number

The **Trade Item Number** represents a specific commercial product or brand under a Generic Item Number.

| Generic Item Number | Trade Item Number | Trade Name |
|---|---|---|
| 1000 | 1001 | Panadol |
| 1000 | 1002 | Fevadol |
| 1000 | 1003 | Adol |

All three Trade Items belong to Generic Item Number `1000`.

## 3.3 SFDA Scope

The SFDA tracking system focuses on human medicines. Medical supplies, general items, and laboratory items may be outside this scope.

The platform must maintain:

```text
SFDARequired = Yes / No / Unknown
```

The Generic Item Number is the main classification level.

---

# 4. Product Intelligence / Master Data

The platform must include a permanent module named **Product Intelligence**.

## 4.1 Generic Product Master

Suggested table: `GenericProductMaster`

Fields:

```text
GenericProductID
GenericItemNumber
GenericDescription
SFDARequired
SFDARequirementSource
FirstSeenDate
LastSeenDate
FirstSeenInSFDA
LastSeenInSFDA
EverSeenInSFDA
ActiveStatus
CreatedAt
CreatedBy
UpdatedAt
UpdatedBy
```

Automatic rule:

```text
If any Trade Item under a Generic appears in SFDA:
    SFDARequired = Yes
    EverSeenInSFDA = Yes
```

Authorized users may manually classify non-drug Generic Items as `SFDARequired = No`.

## 4.2 Trade Product Master

Suggested table: `TradeProductMaster`

Fields:

```text
TradeProductID
TradeItemNumber
TradeName
GenericProductID
GenericItemNumber
GTIN
DrugName
PackageSize
FirstSeenDate
LastSeenDate
FirstSeenInSFDA
LastSeenInSFDA
EverSeenInSFDA
CurrentSFDAStatus
ActiveStatus
CreatedAt
CreatedBy
UpdatedAt
UpdatedBy
```

## 4.3 Batch Master

Suggested table: `BatchMaster`

Fields:

```text
BatchID
TradeProductID
GenericProductID
GenericItemNumber
TradeItemNumber
GTIN
BN
ExpiryDate
FirstSeenDate
LastSeenDate
FirstReceivedDate
LastReceivedDate
FirstDispatchDate
LastDispatchDate
FirstSeenInSFDA
LastSeenInSFDA
EverSeenInSFDA
CurrentSFDAStatus
CreatedAt
UpdatedAt
```

Do not use `BN` alone as a unique key.

Preferred logical key:

```text
TradeItemNumber + BN + ExpiryDate
```

Fallback logical key:

```text
GenericItemNumber + BN + ExpiryDate
```

GTIN should be used as additional confirmation.

---

# 5. Critical Detection Logic

## 5.1 Known Generic, New Trade, Missing from SFDA

Previous receipt:

```text
Generic Item Number = 1000
Trade Item Number = 1001
Trade Name = Panadol
Present in SFDA = Yes
```

The platform learns that Generic `1000` is SFDA-required.

Later receipt:

```text
Generic Item Number = 1000
Trade Item Number = 1002
Trade Name = Fevadol
Present in current SFDA report = No
```

Required alert:

```text
Known SFDA-Required Generic – Current Trade Not Registered in SFDA
```

This alert must appear even when the new Trade Item has never appeared before.

## 5.2 Previously Registered Product, New Receipt Not Registered

If a Product or Batch was previously seen in SFDA but a new receiving transaction appears without a current SFDA record:

```text
Previously Registered Product – New Receipt Not Registered
```

The alert must include:

```text
Generic Item Number
Trade Item Number
Trade Name
GTIN
BN
Expiry Date
Supplier
Inbound Shipment
Received Date
Received Quantity
Previous SFDA Evidence
Current SFDA Status
Run ID
```

## 5.3 Master Data Conflicts

Detect and queue:

```text
Same Trade Item linked to different Generic Items
Same GTIN linked to different Trade Items
Same Batch linked to conflicting Trade Items
Same Generic linked to inconsistent descriptions
Package Size changes
Trade Name changes
```

---

# 6. Historical Data Strategy

The first major production load will cover approximately `2024 to current date`.

Historical periods may be uploaded in separate ranges, and overlapping date ranges must not create duplicates.

## 6.1 Transaction Date vs Reconciliation Date

Every record must store both:

```text
TransactionDate
ReconciliationDate
```

`TransactionDate` is the actual receiving, dispatch, inventory snapshot, or SFDA date.

`ReconciliationDate` is the date and time when the platform processed the record.

## 6.2 Duplicate Prevention

Receiving preferred unique key:

```text
InboundShipment
+ ASNLine
+ TradeItemNumber
+ BN
+ ExpiryDate
```

Dispatch preferred unique key:

```text
SalesOrderNumber
+ OrderLine
+ TradeItemNumber
+ BN
+ ExpiryDate
+ ToAddress
```

Rules:

- Same values: ignore.
- Existing key with changed values: update and audit.
- New transaction: insert.
- Inventory files are snapshots and must include `SnapshotDate`.
- Never sum separate inventory snapshots together.

---

# 7. Required Database Tables

```text
ReconciliationRuns
UploadedFiles
ReceivingTransactions
DispatchTransactions
InventorySnapshots
SFDARecords
AcceptResults
DispatchResults
VarianceResults
GeneratedFiles
GenericProductMaster
TradeProductMaster
BatchMaster
BatchSupplierHistory
BatchSFDAHistory
CustomerMaster
GLNMapping
PackSizeMapping
MasterDataExceptions
AuditLog
```

---

# 8. Main Table Requirements

## ReconciliationRuns

```text
RunID
RunNumber
StartedAt
CompletedAt
RunStatus
RunType
PeriodFrom
PeriodTo
SubmittedBy
SubmittedByEmail
ApplicationVersion
BusinessRuleVersion
FilesCount
TotalInputRows
MasterRows
AcceptRows
DispatchRows
VarianceRows
AcceptFilesCount
DispatchFilesCount
ErrorCode
ErrorMessage
CorrelationID
CreatedAt
```

Statuses:

```text
Pending
Uploading
Validating
Processing
Generating Files
Completed
Completed With Warnings
Failed
Cancelled
```

## ReceivingTransactions

```text
ReceivingTransactionID
RunID
SourceFileID
InboundShipment
ASNLine
SupplierName
SupplierCode
ReceivedDate
GenericItemNumber
GenericDescription
TradeItemNumber
TradeName
GTIN
BN
ExpiryDate
ReceivedQuantity
PackageSize
ReceivedPackages
SFDARequired
SFDARecordFound
ReconciliationStatus
UniqueTransactionKey
CreatedAt
UpdatedAt
```

## DispatchTransactions

```text
DispatchTransactionID
RunID
SourceFileID
SalesOrderNumber
OrderLine
DispatchDate
ToAddress
CustomerID
CustomerName
GLN
CustomerStatus
GenericItemNumber
GenericDescription
TradeItemNumber
TradeName
GTIN
BN
ExpiryDate
DispatchedQuantity
PackageSize
DispatchedPackages
UniqueTransactionKey
CreatedAt
UpdatedAt
```

Customer statuses:

```text
REGISTERED
DUMMY
UNMAPPED
```

## InventorySnapshots

```text
InventorySnapshotID
RunID
SourceFileID
SnapshotDate
GenericItemNumber
GenericDescription
TradeItemNumber
TradeName
GTIN
BN
ExpiryDate
AvailableQuantity
PackageSize
InventoryPackages
UniqueSnapshotKey
CreatedAt
```

## SFDARecords

```text
SFDARecordID
RunID
SourceFileID
SFDAFileDate
GTIN
DrugName
BN
ExpiryDate
Quantity
Active
DeActivated
QuantitySentPending
QuantityReceivePending
Sold
Consumed
Exported
Recalled
Blocked
GenericItemNumber
TradeItemNumber
PackageSize
CreatedAt
```

## AcceptResults

```text
AcceptResultID
RunID
ReceivingTransactionID
GenericItemNumber
TradeItemNumber
GTIN
DrugName
TradeName
BN
ExpiryDate
SupplierName
InboundShipment
ReceivedDate
ReceivedQuantity
PackageSize
ReceivingPackages
Active
QuantityReceivePending
QuantitySentPending
ToBeAccept
SFDAUploadStatus
SFDAUploadDate
GeneratedFileID
CreatedAt
```

## DispatchResults

```text
DispatchResultID
RunID
DispatchTransactionID
GenericItemNumber
TradeItemNumber
GTIN
DrugName
TradeName
BN
ExpiryDate
SalesOrderNumber
OrderLine
DispatchDate
OriginalToAddress
FinalToAddress
CustomerID
CustomerName
GLN
CustomerStatus
DispatchedQuantity
PackageSize
ActualDispatchPackages
CalculatedToBeDispatch
AllocatedToBeDispatch
RemainingToBeDispatch
SFDAUploadStatus
SFDAUploadDate
GeneratedFileID
CreatedAt
```

## VarianceResults

```text
VarianceResultID
RunID
VarianceCategory
VarianceType
VarianceStatus
GenericItemNumber
GenericDescription
TradeItemNumber
TradeName
GTIN
DrugName
BN
ExpiryDate
SupplierName
InboundShipment
ReceivedDate
CustomerName
ToAddress
GLN
SFDARegisteredQuantity
ActualReceivedQuantity
ActualDispatchQuantity
Active
Inventory
Receiving
CalculatedToBeDispatch
AllocatedToBeDispatch
RemainingToBeDispatch
VarianceQuantity
RootCause
Remarks
ResolutionStatus
ResolvedBy
ResolvedAt
CreatedAt
UpdatedAt
```

---

# 9. Variance Classifications

## Receiving Variance

### Under Delivery

```text
Actual Received Quantity < SFDA Quantity Receive Pending
```

### Over Delivery

```text
Actual Received Quantity > SFDA Quantity Receive Pending
```

### Received but Not Registered in SFDA

```text
Actual Received Quantity > 0
and SFDA Quantity Receive Pending = 0
```

### Known SFDA Generic – Missing Current Registration

```text
GenericProductMaster.SFDARequired = Yes
and current Trade or Batch is missing from SFDA
```

## Dispatch Variance

### Missing Dispatch Evidence

```text
Calculated To Be Dispatch > Allocated To Be Dispatch
```

### Unmapped Customer

```text
Actual dispatch exists but customer is absent from GLN mapping
```

### DUMMY Customer

```text
Customer is not found in the approved SFDA customer list
```

---

# 10. Official SFDA Receiving Discrepancy Report

Required columns:

```text
ReportNumber
ReportDate
Warehouse
RunID
SupplierName
SupplierCode
InboundShipment
ReceivedDate
GenericItemNumber
GenericDescription
TradeItemNumber
TradeName
GTIN
DrugName
BN
ExpiryDate
SFDARegisteredQuantity
ActualReceivedQuantity
VarianceQuantity
VarianceType
VarianceDescription
Remarks
SupportingFile
ResolutionStatus
```

Exports:

```text
Excel
PDF
Supplier-specific report
Consolidated period report
```

---

# 11. Drug Journey / Traceability

Users must be able to search a Batch and see:

```text
Supplier
→ ASN Received
→ Accepted in SFDA
→ Stored in Warehouse
→ Dispatched
→ Customer
→ Reported to SFDA
→ Closed / Variance
```

Filters:

```text
Generic Item Number
Trade Item Number
GTIN
BN
Expiry Date
Supplier
Customer
Receiving Date
Dispatch Date
Run ID
```

---

# 12. Web Application Pages

Final navigation:

```text
Home
Upload & Run
Results
History
Reports
Product Intelligence
Variance Management
Administration
```

## Home

Required KPIs:

```text
Last Run Status
Last Run ID
Last Run Date and Time
Processing Duration
Total Received Quantity
Total Received Packages
Total Dispatched Quantity
Total Dispatched Packages
Accept Quantity
Dispatch Quantity
Variance Quantity
Open Variance
Resolved Variance
Dummy Customers
SFDA Required Generics
Missing SFDA Registrations
Generated Files
Data Health Score
```

## Upload & Run

Upload and Run must remain on the same page.

Workflow:

```text
Step 1 – Upload Files
Step 2 – Validate Files
Step 3 – Run Reconciliation
Step 4 – Review and Download Results
```

Each file card must show file name, size, report type, date range, row count, column count, validation status, missing columns, duplicates, Replace, and Remove.

## Results

Tabs:

```text
Overview
Accept
Dispatch
Variance
Customers
Generated Files
Audit
```

Filters:

```text
Transaction date range
Reconciliation date range
Generic Item Number
Trade Item Number
GTIN
BN
Expiry Date
Drug Name
Trade Name
Supplier
Customer
GLN
Run ID
Status
Variance Type
```

## History

Columns:

```text
Run ID
Date and Time
User
Run Type
Period From
Period To
Status
Input Files
Input Rows
Accept
Dispatch
Variance
Duration
Application Version
Actions
```

Actions: View, Download, Retry, Compare, Export, Delete (Admin only).

## Product Intelligence

Views:

```text
Generic Product Master
Trade Product Master
Batch Master
Supplier History
SFDA History
Master Data Exceptions
```

## Variance Management

Statuses:

```text
New
Under Review
Supplier Contacted
SFDA Contacted
Correction Pending
Resolved
Rejected
Closed
```

## Administration

Modules:

```text
GLN Mapping
Pack Size Mapping
Generic Classification
SFDA Required Classification
Business Rules
File Limits
User Roles
Application Version
API Health
Database Health
Storage Health
Audit Logs
```

---

# 13. SFDA Upload File Rules

Header:

```text
GTIN;QUANTITY;BN;XD
```

Rules:

```text
Maximum 20 data rows per file
Maximum total quantity 100,000 per file
Positive quantities only
GTIN exported as text
Leading zeros preserved
Date format DD-MM-YYYY
```

Accept filenames:

```text
Accept_001.csv
```

Dispatch filenames:

```text
Dispatch_<GLN>_<CustomerName>_001.csv
```

DUMMY filename:

```text
Dispatch_DUMMY_DUMMY_CUSTOMER_001.csv
```

---

# 14. Current Reconciliation Logic

Core key:

```text
BN + Expiry Date
```

Receiving:

```text
Receiving Packages = SUM(Received Quantity) ÷ PackageSize
```

Inventory:

```text
Inventory Packages = SUM(Available Quantity) ÷ PackageSize
```

To Be Accept:

```text
If Inventory > Active
and Quantity Sent Pending = 0
and Quantity Receive Pending > 0:
    To Be Accept = MIN(Quantity Receive Pending, Inventory)
Else:
    To Be Accept = MIN(Quantity Receive Pending, Receiving)
```

To Be Dispatch:

```text
If Inventory = 0:
    To Be Dispatch = Active
Else:
    To Be Dispatch = MAX(0, Active - Inventory)
```

Dispatch allocation:

```text
Calculate required dispatch by Batch + Expiry.
Compare with actual WMS dispatch.
Allocate to actual customers using To Address.
Group by registered GLN customer.
Move unallocated quantity to Variance.
```

---

# 15. Future ERP / SAP Integration

The platform must support SAP ERP, Oracle ERP, Java-based ERP, WMS, Power BI, and future SFDA services.

Future architecture:

```text
SAP / ERP / WMS
        ↓
Integration APIs
        ↓
Azure Integration Layer
        ↓
Azure SQL Database
        ↓
Reconciliation Engine
        ↓
Web Application
        ↓
SFDA Upload / Reports / Power BI
```

Excel upload must remain available as a fallback.

---

# 16. Recommended Azure Architecture

```text
Azure Web Application
Azure Functions
Azure SQL Database
Azure Blob Storage
Microsoft Entra ID
Azure Key Vault
Application Insights
API Management (future)
Service Bus (future)
```

Blob Storage will store uploaded files, generated CSVs, ZIPs, Excel reports, PDFs, and supporting attachments.

Azure SQL will store transactions, results, history, master data, variance workflows, audit records, and file metadata.

---

# 17. Security and Audit

Roles:

```text
Viewer
Operator
Exporter
Variance Officer
Master Data Administrator
System Administrator
```

Audit events:

```text
LOGIN
FILE_UPLOADED
FILE_REPLACED
RECONCILIATION_STARTED
RECONCILIATION_COMPLETED
RECONCILIATION_FAILED
ACCEPT_DOWNLOADED
DISPATCH_DOWNLOADED
VARIANCE_EXPORTED
GLN_UPDATED
PACK_SIZE_UPDATED
GENERIC_CLASSIFICATION_UPDATED
VARIANCE_STATUS_UPDATED
RUN_DELETED
```

Every run must preserve input file names and hashes, row counts, application version, business rule version, reference-file versions, user, timestamp, generated outputs, and Correlation ID.

---

# 18. Non-Negotiable Requirements

1. Generic Item Number is the primary indicator for identifying SFDA-required human drugs.
2. Trade Item Number represents a commercial product under the Generic.
3. If one Trade under a Generic appears in SFDA, the Generic becomes known as SFDA-required.
4. A new Trade under a known SFDA-required Generic must be flagged if absent from SFDA.
5. All receiving, dispatch, inventory, SFDA, Accept, Dispatch, and Variance records must be stored historically.
6. Transaction Date and Reconciliation Date must remain separate.
7. Historical uploads must support overlapping periods without duplication.
8. Receiving records must preserve supplier and actual receiving date.
9. Dispatch records must preserve actual customer and GLN.
10. Every Batch must have a searchable complete journey.
11. Variance must include receiving discrepancies and missing SFDA registrations, not only dispatch allocation differences.
12. The system must generate a formal SFDA discrepancy report.
13. Files and results must persist after browser refresh and across devices.
14. The platform must support future SAP / ERP integration.
15. GTIN must remain text and preserve leading zeros.
16. SFDA upload files must remain limited to 20 rows and 100,000 total quantity.
17. Pack Size and GLN mapping must be controlled internal Master Data.
18. All important changes must be audited.

---

# 19. Current Project Status

```text
Azure reconciliation engine: Working
Current web interface: Working prototype
Accept files: Working
Dispatch customer allocation: Working
Variance output: Working
Pack Size internal mapping: Working
GLN internal mapping: Working
Persistent database: Not yet implemented
Persistent file storage: Not yet implemented
Historical data module: Not yet implemented
Product Intelligence: Requirements defined
Variance workflow: Requirements defined
SAP / ERP integration: Future phase
```

---

# 20. Document Purpose

This file is the permanent reference for the project. Update it whenever a requirement, business rule, database field, page, integration, or reconciliation rule changes.
