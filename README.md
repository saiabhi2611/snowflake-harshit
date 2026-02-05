# SQL Server Financial Planning Objects - Snowflake Migration Challenge Set

This directory contains a set of SQL Server database objects designed to test and challenge Snowflake migration tools. These objects are modeled after common enterprise financial planning and budgeting systems.

## Object Inventory

### Tables (8 objects)

| Object | Dependencies | Snowflake Challenges |
|--------|--------------|---------------------|
| `FiscalPeriod` | None | ROWVERSION, filtered indexes, INCLUDE columns |
| `CostCenter` | Self-referential | **HIERARCHYID**, temporal tables (SYSTEM_TIME), computed columns from HIERARCHYID methods |
| `GLAccount` | Self-referential | SPARSE columns, columnstore indexes |
| `BudgetHeader` | FiscalPeriod | XML columns, XML indexes, computed persisted columns |
| `BudgetLineItem` | BudgetHeader, GLAccount, CostCenter, FiscalPeriod | HASHBYTES computed column, filtered indexes, IGNORE_DUP_KEY, columnstore |
| `AllocationRule` | CostCenter | XML columns with complex schemas, PRIMARY XML INDEX |
| `ConsolidationJournal` | BudgetHeader, FiscalPeriod | FILESTREAM, ROWGUIDCOL, NEWSEQUENTIALID() |
| `ConsolidationJournalLine` | ConsolidationJournal, GLAccount, CostCenter, AllocationRule | CASCADE delete, columnstore |

### User-Defined Table Types (3 objects)

| Object | Snowflake Challenge |
|--------|---------------------|
| `BudgetLineItemTableType` | **No Snowflake equivalent** - Table-valued parameters don't exist |
| `AllocationResultTableType` | Must refactor to temp tables, stages, or VARIANT arrays |
| `HierarchyNodeTableType` | Indexes on table types not supported |

### Functions (4 objects)

| Object | Type | Dependencies | Snowflake Challenges |
|--------|------|--------------|---------------------|
| `fn_GetAllocationFactor` | Scalar | CostCenter, BudgetLineItem, GLAccount | SCHEMABINDING, cross-table queries in scalar UDF, different UDF paradigm |
| `fn_GetHierarchyPath` | Scalar | CostCenter | Recursive scalar function, WHILE loops in functions |
| `tvf_GetBudgetVariance` | Inline TVF | All tables | **Inline TVFs** compile into query plan - no equivalent |
| `tvf_ExplodeCostCenterHierarchy` | Multi-statement TVF | CostCenter | **Multi-statement TVFs** with table variables, WHILE loops, SCHEMABINDING |

### Views (2 objects)

| Object | Dependencies | Snowflake Challenges |
|--------|--------------|---------------------|
| `vw_BudgetConsolidationSummary` | All core tables | **Indexed view** (materialized), SCHEMABINDING, aggregate restrictions |
| `vw_AllocationRuleTargets` | AllocationRule, CostCenter | XML .nodes() and .value() methods, CROSS APPLY to XML |

### Stored Procedures (5 objects)

| Object | Dependencies | Lines | Primary Snowflake Challenges |
|--------|--------------|-------|------------------------------|
| `usp_ProcessBudgetConsolidation` | All objects | ~350 | Cursors (FAST_FORWARD, SCROLL, KEYSET), table variables with indexes, nested transactions, savepoints, OUTPUT clause, SCOPE_IDENTITY, TRY-CATCH with THROW, sp_executesql with output params, MERGE |
| `usp_ExecuteCostAllocation` | AllocationRule, BudgetLineItem, Functions | ~300 | sp_getapplock/sp_releaseapplock, WAITFOR DELAY, GOTO statements, STRING_SPLIT, TRY_CONVERT, STRING_AGG with ORDER BY, recursive CTE with cycle detection, CROSS APPLY to views |
| `usp_GenerateRollingForecast` | All tables, Functions | ~280 | Global temp tables (##), OPENJSON, dynamic PIVOT, FOR XML PATH string aggregation, complex window functions (PERCENTILE_CONT, LAG with multiple offsets), OPTION (MAXRECURSION) |
| `usp_ReconcileIntercompanyBalances` | All tables, Views | ~280 | sp_xml_preparedocument/removedocument, OPENXML, HASHBYTES, FOR XML PATH with complex nesting, CROSS APPLY with derived tables |
| `usp_PerformFinancialClose` | All objects including other procedures | ~380 | Nested EXEC with OUTPUT, DISABLE/ENABLE TRIGGER, temporal FOR SYSTEM_TIME, sp_send_dbmail, orchestration pattern |
| `usp_BulkImportBudgetData` | Tables, Types | ~320 | BULK INSERT, FORMAT FILE, OPENROWSET, OPENQUERY (linked servers), table-valued parameters, MERGE with OUTPUT |

## Dependency Graph

```
                                    ┌─────────────────┐
                                    │  FiscalPeriod   │
                                    └────────┬────────┘
                                             │
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
                 ▼                           ▼                           ▼
        ┌─────────────┐            ┌─────────────────┐          ┌─────────────┐
        │  CostCenter │◄───────────│   GLAccount     │          │ BudgetHeader│
        │  (temporal) │            │                 │          │    (XML)    │
        └──────┬──────┘            └────────┬────────┘          └──────┬──────┘
               │                            │                          │
               │  ┌─────────────────────────┼──────────────────────────┤
               │  │                         │                          │
               ▼  ▼                         ▼                          ▼
        ┌─────────────────┐        ┌────────────────────┐      ┌───────────────────┐
        │ AllocationRule  │        │  BudgetLineItem    │◄─────│ConsolidationJournal│
        │     (XML)       │        │   (computed cols)  │      │   (FILESTREAM)    │
        └────────┬────────┘        └────────┬───────────┘      └─────────┬─────────┘
                 │                          │                            │
                 └──────────┬───────────────┴────────────────────────────┤
                            │                                            │
                            ▼                                            ▼
                  ┌───────────────────┐                    ┌─────────────────────────┐
                  │ vw_AllocationRule │                    │ ConsolidationJournalLine│
                  │     Targets       │                    │                         │
                  └───────────────────┘                    └─────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐
│fn_GetAlloc   │  │tvf_Explode       │  │tvf_GetBudget     │
│   Factor     │  │  Hierarchy       │  │   Variance       │
└──────────────┘  └──────────────────┘  └──────────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐
│usp_ProcessBudget │ │usp_ExecuteCost      │ │usp_GenerateRolling   │
│  Consolidation   │ │   Allocation        │ │    Forecast          │
└────────┬─────────┘ └──────────┬──────────┘ └──────────┬───────────┘
         │                      │                       │
         └──────────────────────┼───────────────────────┘
                                │
                                ▼
                  ┌────────────────────────────┐
                  │ usp_ReconcileIntercompany  │
                  │        Balances            │
                  └─────────────┬──────────────┘
                                │
                                ▼
                  ┌────────────────────────────┐
                  │ usp_PerformFinancialClose  │  ◄── Orchestrates ALL procedures
                  │   (Master Orchestrator)    │
                  └────────────────────────────┘
```

## Key Migration Challenges by Category

### 1. Data Types (No Direct Equivalent)
- `HIERARCHYID` - No Snowflake equivalent; requires materialized path or nested set model
- `ROWVERSION` / `TIMESTAMP` - Use CURRENT_TIMESTAMP() or sequences
- `XML` - Use VARIANT with different query syntax (no XQuery)
- `FILESTREAM` / `VARBINARY(MAX)` with external storage - Use external stages

### 2. Procedural Constructs
- **Cursors** - Must refactor to set-based operations or JavaScript UDFs
- **Table Variables** - Use temporary tables
- **WHILE Loops** - Refactor to recursive CTEs or procedural JavaScript
- **GOTO Statements** - Restructure control flow
- **WAITFOR** - No equivalent for delays

### 3. Transaction Management
- Named transactions and savepoints - Limited support
- Nested transactions with `@@TRANCOUNT` - Different semantics
- `XACT_ABORT` and `XACT_STATE()` - Different error model
- Application locks (`sp_getapplock`) - Use Snowflake's own locking

### 4. XML Processing
- `FOR XML PATH` - Use OBJECT_CONSTRUCT and ARRAY_AGG
- `OPENXML` with `sp_xml_preparedocument` - Parse with PARSE_XML or VARIANT
- XML indexes - No equivalent (use VARIANT with FLATTEN)
- XQuery in `.value()`, `.nodes()`, `.query()` - Different syntax

### 5. Dynamic SQL
- `sp_executesql` with output parameters - Use EXECUTE IMMEDIATE
- Dynamic PIVOT - Requires different approach
- `QUOTENAME()` - Use IDENTIFIER()

### 6. Bulk Operations
- `BULK INSERT` - Use `COPY INTO` with stages
- Format files - Define in COPY options
- `OPENROWSET` / `OPENQUERY` - Use external tables or data sharing

### 7. System Integration
- Database Mail (`sp_send_dbmail`) - Use external notifications
- Linked Servers - Use data sharing or external tables
- Service Broker - Use streams and tasks
- Extended stored procedures (`xp_*`) - No equivalent

### 8. Advanced Features
- Temporal tables (`FOR SYSTEM_TIME`) - Use Time Travel (90-day limit)
- Indexed views (materialized) - Use Snowflake materialized views (different semantics)
- Columnstore indexes - Native in Snowflake but automatic
- Filtered indexes - No equivalent; use clustering keys

## Testing Recommendations

1. **Schema Migration First** - Test table and type creation
2. **Function Migration** - Scalar UDFs, then attempt TVF conversion
3. **View Migration** - Check for indexed view semantics
4. **Procedure Migration** - Start with simpler procedures, work up to orchestrators
5. **Integration Testing** - Test the full `usp_PerformFinancialClose` workflow

## Notes

- All objects use the `Planning` schema
- The schema creation (`CREATE SCHEMA Planning`) is not included
- Some features (CLR, extended procedures) are commented out but referenced
- Error handling patterns are intentionally SQL Server-specific
