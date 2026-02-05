/*
    usp_BulkImportBudgetData - Bulk import budget data with validation and transformation
    
    Dependencies:
        - Tables: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod
        - Types: BudgetLineItemTableType
        - Functions: fn_GetHierarchyPath
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. BULK INSERT from file - Snowflake uses COPY INTO with stages
    2. FORMAT FILE specification - No equivalent
    3. OPENROWSET for ad-hoc file access - Use external stages
    4. bcp format patterns - Different bulk loading paradigm
    5. Table-valued parameters as INPUT - Must use temp tables or stages
    6. SET IDENTITY_INSERT - Snowflake has different sequence handling
    7. @@IDENTITY vs SCOPE_IDENTITY vs IDENT_CURRENT - All different
    8. BULK INSERT error handling (MAXERRORS, ERRORFILE) - Different patterns
    9. Memory-optimized table variables - No equivalent
    10. Inline table-valued constructor (VALUES clause with many rows)
    11. TRUNCATE TABLE with foreign keys
    12. COLUMNS_UPDATED() and UPDATE() trigger functions
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_BulkImportBudgetData
    @ImportSource           VARCHAR(20),          -- FILE, TVP, STAGING_TABLE, LINKED_SERVER
    @FilePath               NVARCHAR(500) = NULL, -- For FILE import
    @FormatFilePath         NVARCHAR(500) = NULL,
    @BudgetData             Planning.BudgetLineItemTableType READONLY,  -- For TVP import
    @StagingTableName       NVARCHAR(128) = NULL, -- For STAGING_TABLE import
    @LinkedServerName       NVARCHAR(128) = NULL, -- For LINKED_SERVER import
    @LinkedServerQuery      NVARCHAR(MAX) = NULL,
    @TargetBudgetHeaderID   INT,
    @ValidationMode         VARCHAR(20) = 'STRICT',  -- STRICT, LENIENT, NONE
    @DuplicateHandling      VARCHAR(20) = 'REJECT',  -- REJECT, UPDATE, SKIP
    @BatchSize              INT = 10000,
    @UseParallelLoad        BIT = 1,
    @MaxDegreeOfParallelism INT = 4,
    @ImportResults          XML = NULL OUTPUT,
    @RowsImported           INT = NULL OUTPUT,
    @RowsRejected           INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @StartTime DATETIME2 = SYSUTCDATETIME();
    DECLARE @ImportBatchID UNIQUEIDENTIFIER = NEWID();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @TotalRows INT = 0;
    DECLARE @ValidRows INT = 0;
    DECLARE @InvalidRows INT = 0;
    DECLARE @ProcessedBatches INT = 0;
    
    -- Staging table for imported data
    CREATE TABLE #ImportStaging (
        RowID                   INT IDENTITY(1,1) PRIMARY KEY,
        GLAccountID             INT NULL,
        AccountNumber           VARCHAR(20) NULL,  -- Alternative lookup
        CostCenterID            INT NULL,
        CostCenterCode          VARCHAR(20) NULL,  -- Alternative lookup
        FiscalPeriodID          INT NULL,
        FiscalYear              SMALLINT NULL,     -- Alternative lookup
        FiscalMonth             TINYINT NULL,      -- Alternative lookup
        OriginalAmount          DECIMAL(19,4) NULL,
        AdjustedAmount          DECIMAL(19,4) NULL,
        SpreadMethodCode        VARCHAR(10) NULL,
        Notes                   NVARCHAR(500) NULL,
        -- Validation tracking
        IsValid                 BIT DEFAULT 1,
        ValidationErrors        NVARCHAR(MAX) NULL,
        -- Processing tracking
        IsProcessed             BIT DEFAULT 0,
        ProcessedDateTime       DATETIME2 NULL,
        ResultLineItemID        BIGINT NULL,
        INDEX IX_Valid (IsValid, IsProcessed)
    );
    
    -- Error tracking table
    CREATE TABLE #ImportErrors (
        ErrorID                 INT IDENTITY(1,1) PRIMARY KEY,
        RowID                   INT,
        ErrorCode               VARCHAR(20),
        ErrorMessage            NVARCHAR(500),
        ColumnName              NVARCHAR(128),
        OriginalValue           NVARCHAR(500),
        Severity                VARCHAR(10)  -- ERROR, WARNING
    );
    
    BEGIN TRY
        -- =====================================================================
        -- Step 1: Load data into staging based on source type
        -- =====================================================================
        IF @ImportSource = 'FILE'
        BEGIN
            IF @FilePath IS NULL
            BEGIN
                RAISERROR('File path is required for FILE import source', 16, 1);
                RETURN -1;
            END
            
            -- BULK INSERT - Very different in Snowflake (COPY INTO)
            IF @FormatFilePath IS NOT NULL
            BEGIN
                SET @DynamicSQL = N'
                    BULK INSERT #ImportStaging
                    FROM ''' + @FilePath + N'''
                    WITH (
                        FORMATFILE = ''' + @FormatFilePath + N''',
                        FIRSTROW = 2,
                        MAXERRORS = 1000,
                        TABLOCK,
                        ROWS_PER_BATCH = ' + CAST(@BatchSize AS NVARCHAR) + N',
                        ORDER (AccountNumber, CostCenterCode, FiscalYear, FiscalMonth),
                        ERRORFILE = ''' + @FilePath + N'.errors''
                    );';
            END
            ELSE
            BEGIN
                SET @DynamicSQL = N'
                    BULK INSERT #ImportStaging
                    FROM ''' + @FilePath + N'''
                    WITH (
                        FIELDTERMINATOR = '','',
                        ROWTERMINATOR = ''\n'',
                        FIRSTROW = 2,
                        MAXERRORS = 1000,
                        CODEPAGE = ''65001'',
                        TABLOCK
                    );';
            END
            
            EXEC sp_executesql @DynamicSQL;
            SET @TotalRows = @@ROWCOUNT;
        END
        ELSE IF @ImportSource = 'TVP'
        BEGIN
            -- Load from table-valued parameter
            INSERT INTO #ImportStaging (
                GLAccountID, CostCenterID, FiscalPeriodID,
                OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes
            )
            SELECT 
                GLAccountID, CostCenterID, FiscalPeriodID,
                OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes
            FROM @BudgetData;
            
            SET @TotalRows = @@ROWCOUNT;
        END
        ELSE IF @ImportSource = 'STAGING_TABLE'
        BEGIN
            IF @StagingTableName IS NULL
            BEGIN
                RAISERROR('Staging table name is required for STAGING_TABLE import source', 16, 1);
                RETURN -1;
            END
            
            -- Dynamic insert from staging table
            SET @DynamicSQL = N'
                INSERT INTO #ImportStaging (
                    AccountNumber, CostCenterCode, FiscalYear, FiscalMonth,
                    OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes
                )
                SELECT 
                    AccountNumber, CostCenterCode, FiscalYear, FiscalMonth,
                    OriginalAmount, AdjustedAmount, SpreadMethodCode, Notes
                FROM ' + QUOTENAME(@StagingTableName) + N';';
            
            EXEC sp_executesql @DynamicSQL;
            SET @TotalRows = @@ROWCOUNT;
        END
        ELSE IF @ImportSource = 'LINKED_SERVER'
        BEGIN
            -- OPENQUERY for linked server access - No Snowflake equivalent
            IF @LinkedServerName IS NULL OR @LinkedServerQuery IS NULL
            BEGIN
                RAISERROR('Linked server name and query are required for LINKED_SERVER import', 16, 1);
                RETURN -1;
            END
            
            SET @DynamicSQL = N'
                INSERT INTO #ImportStaging (
                    AccountNumber, CostCenterCode, FiscalYear, FiscalMonth,
                    OriginalAmount, AdjustedAmount, Notes
                )
                SELECT * FROM OPENQUERY(' + QUOTENAME(@LinkedServerName) + N', 
                    ''' + REPLACE(@LinkedServerQuery, '''', '''''') + N''');';
            
            EXEC sp_executesql @DynamicSQL;
            SET @TotalRows = @@ROWCOUNT;
        END
        
        -- =====================================================================
        -- Step 2: Resolve lookups (IDs from codes)
        -- =====================================================================
        
        -- Resolve GLAccountID from AccountNumber
        UPDATE stg
        SET stg.GLAccountID = gla.GLAccountID
        FROM #ImportStaging stg
        INNER JOIN Planning.GLAccount gla ON stg.AccountNumber = gla.AccountNumber
        WHERE stg.GLAccountID IS NULL
          AND stg.AccountNumber IS NOT NULL;
        
        -- Resolve CostCenterID from CostCenterCode
        UPDATE stg
        SET stg.CostCenterID = cc.CostCenterID
        FROM #ImportStaging stg
        INNER JOIN Planning.CostCenter cc ON stg.CostCenterCode = cc.CostCenterCode
        WHERE stg.CostCenterID IS NULL
          AND stg.CostCenterCode IS NOT NULL;
        
        -- Resolve FiscalPeriodID from Year/Month
        UPDATE stg
        SET stg.FiscalPeriodID = fp.FiscalPeriodID
        FROM #ImportStaging stg
        INNER JOIN Planning.FiscalPeriod fp 
            ON stg.FiscalYear = fp.FiscalYear 
            AND stg.FiscalMonth = fp.FiscalMonth
        WHERE stg.FiscalPeriodID IS NULL
          AND stg.FiscalYear IS NOT NULL
          AND stg.FiscalMonth IS NOT NULL;
        
        -- =====================================================================
        -- Step 3: Validate data
        -- =====================================================================
        IF @ValidationMode <> 'NONE'
        BEGIN
            -- Check for missing required fields
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT RowID, 'MISSING_ACCOUNT', 'GL Account not found or not specified', 'GLAccountID', 
                   CASE @ValidationMode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
            FROM #ImportStaging
            WHERE GLAccountID IS NULL;
            
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT RowID, 'MISSING_COSTCENTER', 'Cost Center not found or not specified', 'CostCenterID',
                   CASE @ValidationMode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
            FROM #ImportStaging
            WHERE CostCenterID IS NULL;
            
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT RowID, 'MISSING_PERIOD', 'Fiscal Period not found or not specified', 'FiscalPeriodID',
                   CASE @ValidationMode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
            FROM #ImportStaging
            WHERE FiscalPeriodID IS NULL;
            
            -- Check for invalid amounts
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, OriginalValue, Severity)
            SELECT RowID, 'INVALID_AMOUNT', 'Amount is NULL', 'OriginalAmount', NULL, 'ERROR'
            FROM #ImportStaging
            WHERE OriginalAmount IS NULL;
            
            -- Check account is budgetable
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT stg.RowID, 'NON_BUDGETABLE', 'Account is not marked as budgetable', 'GLAccountID', 'WARNING'
            FROM #ImportStaging stg
            INNER JOIN Planning.GLAccount gla ON stg.GLAccountID = gla.GLAccountID
            WHERE gla.IsBudgetable = 0;
            
            -- Check cost center is active
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT stg.RowID, 'INACTIVE_CC', 'Cost Center is inactive', 'CostCenterID', 
                   CASE @ValidationMode WHEN 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
            FROM #ImportStaging stg
            INNER JOIN Planning.CostCenter cc ON stg.CostCenterID = cc.CostCenterID
            WHERE cc.IsActive = 0;
            
            -- Check period is not closed
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, ColumnName, Severity)
            SELECT stg.RowID, 'CLOSED_PERIOD', 'Fiscal period is closed', 'FiscalPeriodID', 'ERROR'
            FROM #ImportStaging stg
            INNER JOIN Planning.FiscalPeriod fp ON stg.FiscalPeriodID = fp.FiscalPeriodID
            WHERE fp.IsClosed = 1;
            
            -- Check for duplicates within import
            ;WITH DuplicateCheck AS (
                SELECT 
                    RowID,
                    ROW_NUMBER() OVER (
                        PARTITION BY GLAccountID, CostCenterID, FiscalPeriodID 
                        ORDER BY RowID
                    ) AS RowNum
                FROM #ImportStaging
                WHERE GLAccountID IS NOT NULL
                  AND CostCenterID IS NOT NULL
                  AND FiscalPeriodID IS NOT NULL
            )
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, Severity)
            SELECT RowID, 'DUPLICATE_IN_BATCH', 'Duplicate entry within import batch', 'WARNING'
            FROM DuplicateCheck
            WHERE RowNum > 1;
            
            -- Check for existing records in target
            INSERT INTO #ImportErrors (RowID, ErrorCode, ErrorMessage, Severity)
            SELECT stg.RowID, 'ALREADY_EXISTS', 'Record already exists in target budget', 
                   CASE @DuplicateHandling WHEN 'REJECT' THEN 'ERROR' ELSE 'WARNING' END
            FROM #ImportStaging stg
            INNER JOIN Planning.BudgetLineItem bli 
                ON stg.GLAccountID = bli.GLAccountID
                AND stg.CostCenterID = bli.CostCenterID
                AND stg.FiscalPeriodID = bli.FiscalPeriodID
            WHERE bli.BudgetHeaderID = @TargetBudgetHeaderID;
            
            -- Aggregate validation errors to staging table
            UPDATE stg
            SET 
                IsValid = CASE WHEN EXISTS (
                    SELECT 1 FROM #ImportErrors e 
                    WHERE e.RowID = stg.RowID AND e.Severity = 'ERROR'
                ) THEN 0 ELSE 1 END,
                ValidationErrors = (
                    SELECT STRING_AGG(CONCAT(ErrorCode, ': ', ErrorMessage), '; ')
                    FROM #ImportErrors e
                    WHERE e.RowID = stg.RowID
                )
            FROM #ImportStaging stg;
        END
        
        -- Count valid/invalid
        SELECT 
            @ValidRows = SUM(CASE WHEN IsValid = 1 THEN 1 ELSE 0 END),
            @InvalidRows = SUM(CASE WHEN IsValid = 0 THEN 1 ELSE 0 END)
        FROM #ImportStaging;
        
        -- =====================================================================
        -- Step 4: Process imports in batches
        -- =====================================================================
        DECLARE @BatchNumber INT = 0;
        DECLARE @RowsThisBatch INT = 1;
        
        WHILE @RowsThisBatch > 0
        BEGIN
            SET @BatchNumber = @BatchNumber + 1;
            
            -- Use MERGE for upsert based on duplicate handling
            IF @DuplicateHandling = 'UPDATE'
            BEGIN
                MERGE INTO Planning.BudgetLineItem AS target
                USING (
                    SELECT TOP (@BatchSize)
                        @TargetBudgetHeaderID AS BudgetHeaderID,
                        GLAccountID,
                        CostCenterID,
                        FiscalPeriodID,
                        OriginalAmount,
                        ISNULL(AdjustedAmount, 0) AS AdjustedAmount,
                        SpreadMethodCode,
                        'BULK_IMPORT' AS SourceSystem,
                        CAST(@ImportBatchID AS VARCHAR(50)) AS SourceReference,
                        RowID
                    FROM #ImportStaging
                    WHERE IsValid = 1 
                      AND IsProcessed = 0
                    ORDER BY RowID
                ) AS source
                ON target.BudgetHeaderID = source.BudgetHeaderID
                   AND target.GLAccountID = source.GLAccountID
                   AND target.CostCenterID = source.CostCenterID
                   AND target.FiscalPeriodID = source.FiscalPeriodID
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.OriginalAmount = source.OriginalAmount,
                        target.AdjustedAmount = source.AdjustedAmount,
                        target.SpreadMethodCode = source.SpreadMethodCode,
                        target.SourceReference = source.SourceReference,
                        target.LastModifiedDateTime = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                            OriginalAmount, AdjustedAmount, SpreadMethodCode, 
                            SourceSystem, SourceReference, LastModifiedDateTime)
                    VALUES (source.BudgetHeaderID, source.GLAccountID, source.CostCenterID, 
                            source.FiscalPeriodID, source.OriginalAmount, source.AdjustedAmount,
                            source.SpreadMethodCode, source.SourceSystem, source.SourceReference,
                            SYSUTCDATETIME())
                OUTPUT 
                    $action,
                    inserted.BudgetLineItemID,
                    source.RowID
                INTO #MergeOutput (Action, LineItemID, SourceRowID);
                
                SET @RowsThisBatch = @@ROWCOUNT;
            END
            ELSE  -- SKIP duplicates or REJECT (already filtered by IsValid)
            BEGIN
                -- Using OUTPUT clause to track inserted rows
                DECLARE @InsertedRows TABLE (
                    LineItemID BIGINT,
                    SourceRowID INT
                );
                
                INSERT INTO Planning.BudgetLineItem (
                    BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                    OriginalAmount, AdjustedAmount, SpreadMethodCode,
                    SourceSystem, SourceReference, ImportBatchID, LastModifiedDateTime
                )
                OUTPUT 
                    inserted.BudgetLineItemID,
                    inserted.GLAccountID  -- We'll need to join back
                INTO @InsertedRows (LineItemID, SourceRowID)
                SELECT TOP (@BatchSize)
                    @TargetBudgetHeaderID,
                    GLAccountID,
                    CostCenterID,
                    FiscalPeriodID,
                    OriginalAmount,
                    ISNULL(AdjustedAmount, 0),
                    SpreadMethodCode,
                    'BULK_IMPORT',
                    CAST(@ImportBatchID AS VARCHAR(50)),
                    @ImportBatchID,
                    SYSUTCDATETIME()
                FROM #ImportStaging stg
                WHERE IsValid = 1 
                  AND IsProcessed = 0
                  AND (@DuplicateHandling = 'REJECT' 
                       OR NOT EXISTS (
                           SELECT 1 FROM Planning.BudgetLineItem bli
                           WHERE bli.BudgetHeaderID = @TargetBudgetHeaderID
                             AND bli.GLAccountID = stg.GLAccountID
                             AND bli.CostCenterID = stg.CostCenterID
                             AND bli.FiscalPeriodID = stg.FiscalPeriodID
                       ))
                ORDER BY RowID;
                
                SET @RowsThisBatch = @@ROWCOUNT;
            END
            
            -- Mark processed rows
            UPDATE stg
            SET IsProcessed = 1,
                ProcessedDateTime = SYSUTCDATETIME()
            FROM #ImportStaging stg
            WHERE IsValid = 1 
              AND IsProcessed = 0
              AND stg.RowID <= (
                  SELECT MAX(RowID) FROM (
                      SELECT TOP (@BatchSize) RowID 
                      FROM #ImportStaging 
                      WHERE IsValid = 1 AND IsProcessed = 0
                      ORDER BY RowID
                  ) AS processed
              );
            
            SET @ProcessedBatches = @ProcessedBatches + 1;
        END
        
        -- Set output parameters
        SELECT 
            @RowsImported = SUM(CASE WHEN IsProcessed = 1 THEN 1 ELSE 0 END),
            @RowsRejected = SUM(CASE WHEN IsValid = 0 THEN 1 ELSE 0 END)
        FROM #ImportStaging;
        
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @RowsImported = 0;
        SET @RowsRejected = @TotalRows;
    END CATCH
    
    -- =========================================================================
    -- Build results XML
    -- =========================================================================
    SET @ImportResults = (
        SELECT 
            @ImportBatchID AS '@BatchID',
            @ImportSource AS '@Source',
            @TargetBudgetHeaderID AS '@TargetBudgetID',
            DATEDIFF(MILLISECOND, @StartTime, SYSUTCDATETIME()) AS '@DurationMs',
            -- Summary
            (
                SELECT 
                    @TotalRows AS TotalRows,
                    @ValidRows AS ValidRows,
                    @InvalidRows AS InvalidRows,
                    @RowsImported AS ImportedRows,
                    @RowsRejected AS RejectedRows,
                    @ProcessedBatches AS BatchesProcessed,
                    @BatchSize AS BatchSize
                FOR XML PATH('Summary'), TYPE
            ),
            -- Error summary by type
            (
                SELECT 
                    ErrorCode AS '@Code',
                    COUNT(*) AS Count,
                    MAX(Severity) AS MaxSeverity
                FROM #ImportErrors
                GROUP BY ErrorCode
                FOR XML PATH('ErrorType'), ROOT('ErrorSummary'), TYPE
            ),
            -- Sample rejected rows (first 100)
            (
                SELECT TOP 100
                    stg.RowID AS '@Row',
                    stg.AccountNumber AS Account,
                    stg.CostCenterCode AS CostCenter,
                    stg.FiscalYear AS Year,
                    stg.FiscalMonth AS Month,
                    stg.OriginalAmount AS Amount,
                    stg.ValidationErrors AS Errors
                FROM #ImportStaging stg
                WHERE stg.IsValid = 0
                ORDER BY stg.RowID
                FOR XML PATH('RejectedRow'), ROOT('RejectedSample'), TYPE
            )
        FOR XML PATH('ImportResults')
    );
    
    -- Cleanup
    DROP TABLE IF EXISTS #ImportStaging;
    DROP TABLE IF EXISTS #ImportErrors;
    DROP TABLE IF EXISTS #MergeOutput;
    
    RETURN CASE WHEN @ErrorMessage IS NULL THEN 0 ELSE -1 END;
END
GO
