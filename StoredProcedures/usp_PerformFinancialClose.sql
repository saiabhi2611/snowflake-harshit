/*
    usp_PerformFinancialClose - Comprehensive period-end close orchestration procedure
    
    Dependencies:
        - Tables: All tables in the Planning schema
        - Views: All views in the Planning schema  
        - Functions: All functions in the Planning schema
        - Procedures: usp_ProcessBudgetConsolidation, usp_ExecuteCostAllocation, 
                      usp_ReconcileIntercompanyBalances
        - Types: BudgetLineItemTableType, AllocationResultTableType
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. Service Broker messaging (commented but shows pattern)
    2. EXEC nested procedures with OUTPUT params
    3. Transaction nesting with @@TRANCOUNT checks
    4. Multiple RETURN points with different codes
    5. DISABLE/ENABLE TRIGGER for maintenance
    6. CHECKPOINT and DBCC SHRINKDATABASE (admin operations)
    7. sp_OA* automation procedures
    8. Send email via sp_send_dbmail
    9. Agent job scheduling via msdb procedures
    10. Change Data Capture (CDC) queries
    11. Extended events session management
    12. Query Store operations
    13. Memory-optimized tables with NATIVE_COMPILATION
    14. Temporal table FOR SYSTEM_TIME queries
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_PerformFinancialClose
    @FiscalPeriodID             INT,
    @CloseType                  VARCHAR(20) = 'SOFT',     -- SOFT, HARD, FINAL
    @RunConsolidation           BIT = 1,
    @RunAllocations             BIT = 1,
    @RunReconciliation          BIT = 1,
    @SendNotifications          BIT = 1,
    @NotificationRecipients     NVARCHAR(MAX) = NULL,     -- Semicolon-separated emails
    @ForceClose                 BIT = 0,
    @ClosingUserID              INT,
    @CloseResults               XML = NULL OUTPUT,
    @OverallStatus              VARCHAR(20) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;  -- We handle errors manually for orchestration
    
    -- =========================================================================
    -- Declarations and initialization
    -- =========================================================================
    DECLARE @ProcStartTime DATETIME2(7) = SYSUTCDATETIME();
    DECLARE @StepStartTime DATETIME2(7);
    DECLARE @CurrentStep NVARCHAR(100);
    DECLARE @CloseRunID UNIQUEIDENTIFIER = NEWID();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ReturnCode INT = 0;
    DECLARE @FiscalYear SMALLINT;
    DECLARE @FiscalMonth TINYINT;
    DECLARE @PeriodName NVARCHAR(50);
    DECLARE @IsAlreadyClosed BIT;
    
    -- Step tracking
    DECLARE @StepResults TABLE (
        StepNumber          INT IDENTITY(1,1) PRIMARY KEY,
        StepName            NVARCHAR(100),
        StartTime           DATETIME2(7),
        EndTime             DATETIME2(7),
        DurationMs          INT,
        Status              VARCHAR(20),
        RowsAffected        INT,
        ErrorMessage        NVARCHAR(4000),
        OutputData          XML
    );
    
    -- Nested procedure output variables
    DECLARE @ConsolidationBudgetID INT;
    DECLARE @ConsolidationRows INT;
    DECLARE @ConsolidationError NVARCHAR(4000);
    DECLARE @AllocationRows INT;
    DECLARE @AllocationWarnings NVARCHAR(MAX);
    DECLARE @ReconciliationXML XML;
    DECLARE @UnreconciledCount INT;
    DECLARE @VarianceTotal DECIMAL(19,4);
    DECLARE @ActiveBudgetID INT;
    
    -- Validation result table
    DECLARE @ValidationErrors TABLE (
        ErrorCode           VARCHAR(20),
        ErrorMessage        NVARCHAR(500),
        Severity            VARCHAR(10),  -- ERROR, WARNING
        BlocksClose         BIT
    );
    
    BEGIN TRY
        -- =====================================================================
        -- Step 1: Validate period and prerequisites
        -- =====================================================================
        SET @CurrentStep = 'Period Validation';
        SET @StepStartTime = SYSUTCDATETIME();
        
        SELECT 
            @FiscalYear = FiscalYear,
            @FiscalMonth = FiscalMonth,
            @PeriodName = PeriodName,
            @IsAlreadyClosed = IsClosed
        FROM Planning.FiscalPeriod
        WHERE FiscalPeriodID = @FiscalPeriodID;
        
        IF @FiscalYear IS NULL
        BEGIN
            SET @ErrorMessage = 'Fiscal period not found: ' + CAST(@FiscalPeriodID AS VARCHAR);
            INSERT INTO @ValidationErrors VALUES ('INVALID_PERIOD', @ErrorMessage, 'ERROR', 1);
        END
        
        IF @IsAlreadyClosed = 1 AND @ForceClose = 0
        BEGIN
            SET @ErrorMessage = 'Period is already closed. Use @ForceClose=1 to reprocess.';
            INSERT INTO @ValidationErrors VALUES ('ALREADY_CLOSED', @ErrorMessage, 'ERROR', 1);
        END
        
        -- Check prior periods are closed (for HARD and FINAL close)
        IF @CloseType IN ('HARD', 'FINAL')
        BEGIN
            IF EXISTS (
                SELECT 1 FROM Planning.FiscalPeriod
                WHERE FiscalYear = @FiscalYear
                  AND FiscalMonth < @FiscalMonth
                  AND IsClosed = 0
                  AND IsAdjustmentPeriod = 0
            )
            BEGIN
                SET @ErrorMessage = 'Prior periods must be closed before ' + @CloseType + ' close';
                INSERT INTO @ValidationErrors VALUES ('PRIOR_OPEN', @ErrorMessage, 'ERROR', 1);
            END
        END
        
        -- Check for pending journals
        DECLARE @PendingJournals INT;
        SELECT @PendingJournals = COUNT(*)
        FROM Planning.ConsolidationJournal cj
        INNER JOIN Planning.FiscalPeriod fp ON cj.FiscalPeriodID = fp.FiscalPeriodID
        WHERE fp.FiscalPeriodID = @FiscalPeriodID
          AND cj.StatusCode IN ('DRAFT', 'SUBMITTED');
        
        IF @PendingJournals > 0
        BEGIN
            SET @ErrorMessage = CONCAT(@PendingJournals, ' pending journal(s) must be posted or rejected');
            INSERT INTO @ValidationErrors VALUES ('PENDING_JOURNALS', @ErrorMessage, 
                CASE WHEN @CloseType = 'FINAL' THEN 'ERROR' ELSE 'WARNING' END,
                CASE WHEN @CloseType = 'FINAL' THEN 1 ELSE 0 END);
        END
        
        -- Log validation step
        INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                CASE WHEN EXISTS (SELECT 1 FROM @ValidationErrors WHERE BlocksClose = 1) THEN 'FAILED' ELSE 'COMPLETED' END,
                (SELECT COUNT(*) FROM @ValidationErrors));
        
        -- Stop if blocking errors
        IF EXISTS (SELECT 1 FROM @ValidationErrors WHERE BlocksClose = 1)
        BEGIN
            SET @OverallStatus = 'VALIDATION_FAILED';
            GOTO BuildResults;
        END
        
        -- =====================================================================
        -- Step 2: Create snapshot point (using temporal table)
        -- =====================================================================
        SET @CurrentStep = 'Create Snapshot';
        SET @StepStartTime = SYSUTCDATETIME();
        
        -- Query cost center history using FOR SYSTEM_TIME
        -- This temporal query syntax doesn't exist in Snowflake
        DECLARE @SnapshotTime DATETIME2(7) = SYSUTCDATETIME();
        
        SELECT 
            cc.CostCenterID,
            cc.CostCenterCode,
            cc.CostCenterName,
            cc.ParentCostCenterID,
            cc.AllocationWeight,
            'CURRENT' AS SnapshotType
        INTO #CostCenterSnapshot
        FROM Planning.CostCenter 
        FOR SYSTEM_TIME AS OF @SnapshotTime cc  -- Temporal query
        WHERE cc.IsActive = 1;
        
        INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()), 'COMPLETED', @@ROWCOUNT);
        
        -- =====================================================================
        -- Step 3: Run Consolidation (nested procedure call)
        -- =====================================================================
        IF @RunConsolidation = 1
        BEGIN
            SET @CurrentStep = 'Budget Consolidation';
            SET @StepStartTime = SYSUTCDATETIME();
            
            -- Find active budget for this period
            SELECT TOP 1 @ActiveBudgetID = BudgetHeaderID
            FROM Planning.BudgetHeader bh
            INNER JOIN Planning.FiscalPeriod fp ON 
                fp.FiscalPeriodID BETWEEN bh.StartPeriodID AND bh.EndPeriodID
            WHERE fp.FiscalPeriodID = @FiscalPeriodID
              AND bh.StatusCode IN ('APPROVED', 'LOCKED')
            ORDER BY bh.VersionNumber DESC;
            
            IF @ActiveBudgetID IS NOT NULL
            BEGIN
                BEGIN TRY
                    -- Nested procedure execution with OUTPUT parameters
                    EXEC @ReturnCode = Planning.usp_ProcessBudgetConsolidation
                        @SourceBudgetHeaderID = @ActiveBudgetID,
                        @TargetBudgetHeaderID = @ConsolidationBudgetID OUTPUT,
                        @ConsolidationType = 'FULL',
                        @IncludeEliminations = 1,
                        @RecalculateAllocations = 0,
                        @UserID = @ClosingUserID,
                        @RowsProcessed = @ConsolidationRows OUTPUT,
                        @ErrorMessage = @ConsolidationError OUTPUT;
                    
                    INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage)
                    VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                            DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                            CASE WHEN @ReturnCode = 0 THEN 'COMPLETED' ELSE 'WARNING' END,
                            @ConsolidationRows, @ConsolidationError);
                END TRY
                BEGIN CATCH
                    INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                    VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                            DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                            'FAILED', ERROR_MESSAGE());
                    
                    IF @CloseType = 'FINAL'
                        THROW;
                END CATCH
            END
            ELSE
            BEGIN
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                        'SKIPPED', 'No active budget found for period');
            END
        END
        
        -- =====================================================================
        -- Step 4: Run Cost Allocations
        -- =====================================================================
        IF @RunAllocations = 1
        BEGIN
            SET @CurrentStep = 'Cost Allocations';
            SET @StepStartTime = SYSUTCDATETIME();
            
            -- Create empty TVP for allocation results
            DECLARE @EmptyAllocResults Planning.AllocationResultTableType;
            DECLARE @EffectiveBudgetID INT = ISNULL(@ConsolidationBudgetID, @ActiveBudgetID);
            
            BEGIN TRY
                EXEC @ReturnCode = Planning.usp_ExecuteCostAllocation
                    @BudgetHeaderID = @EffectiveBudgetID,
                    @AllocationRuleIDs = NULL,
                    @FiscalPeriodID = @FiscalPeriodID,
                    @DryRun = 0,
                    @ConcurrencyMode = 'EXCLUSIVE',
                    @AllocationResults = @EmptyAllocResults,
                    @RowsAllocated = @AllocationRows OUTPUT,
                    @WarningMessages = @AllocationWarnings OUTPUT;
                
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                        CASE WHEN @ReturnCode = 0 THEN 'COMPLETED' ELSE 'WARNING' END,
                        @AllocationRows, @AllocationWarnings);
            END TRY
            BEGIN CATCH
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                        'FAILED', ERROR_MESSAGE());
                
                IF @CloseType = 'FINAL'
                    THROW;
            END CATCH
        END
        
        -- =====================================================================
        -- Step 5: Run Intercompany Reconciliation
        -- =====================================================================
        IF @RunReconciliation = 1
        BEGIN
            SET @CurrentStep = 'Intercompany Reconciliation';
            SET @StepStartTime = SYSUTCDATETIME();
            
            DECLARE @ReconcileBudgetID INT = ISNULL(@ConsolidationBudgetID, @ActiveBudgetID);
            DECLARE @AutoCreateAdj BIT = CASE WHEN @CloseType = 'FINAL' THEN 0 ELSE 1 END;
            
            BEGIN TRY
                EXEC @ReturnCode = Planning.usp_ReconcileIntercompanyBalances
                    @BudgetHeaderID = @ReconcileBudgetID,
                    @ReconciliationDate = NULL,
                    @EntityCodes = NULL,
                    @ToleranceAmount = 0.01,
                    @AutoCreateAdjustments = @AutoCreateAdj,
                    @ReconciliationReportXML = @ReconciliationXML OUTPUT,
                    @UnreconciledCount = @UnreconciledCount OUTPUT,
                    @TotalVarianceAmount = @VarianceTotal OUTPUT;
                
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected, ErrorMessage, OutputData)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                        CASE 
                            WHEN @UnreconciledCount = 0 THEN 'COMPLETED'
                            WHEN @CloseType = 'FINAL' AND @UnreconciledCount > 0 THEN 'FAILED'
                            ELSE 'WARNING'
                        END,
                        @UnreconciledCount,
                        CASE WHEN @UnreconciledCount > 0 
                             THEN CONCAT(@UnreconciledCount, ' unreconciled items, variance: ', FORMAT(@VarianceTotal, 'C'))
                             ELSE NULL END,
                        @ReconciliationXML);
                
                -- Block FINAL close if unreconciled
                IF @CloseType = 'FINAL' AND @UnreconciledCount > 0
                BEGIN
                    SET @ErrorMessage = 'Cannot perform FINAL close with unreconciled intercompany balances';
                    THROW 50200, @ErrorMessage, 1;
                END
            END TRY
            BEGIN CATCH
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()),
                        'FAILED', ERROR_MESSAGE());
                
                IF @CloseType = 'FINAL'
                    THROW;
            END CATCH
        END
        
        -- =====================================================================
        -- Step 6: Lock the period
        -- =====================================================================
        SET @CurrentStep = 'Lock Period';
        SET @StepStartTime = SYSUTCDATETIME();
        
        BEGIN TRANSACTION LockPeriodTran;
        
        -- Disable triggers during close (SQL Server specific)
        -- This pattern has no Snowflake equivalent
        EXEC('DISABLE TRIGGER ALL ON Planning.BudgetLineItem');
        
        BEGIN TRY
            UPDATE Planning.FiscalPeriod
            SET 
                IsClosed = 1,
                ClosedByUserID = @ClosingUserID,
                ClosedDateTime = SYSUTCDATETIME(),
                ModifiedDateTime = SYSUTCDATETIME()
            WHERE FiscalPeriodID = @FiscalPeriodID;
            
            -- Lock all budgets in this period
            UPDATE Planning.BudgetHeader
            SET 
                StatusCode = 'LOCKED',
                LockedDateTime = SYSUTCDATETIME(),
                ModifiedDateTime = SYSUTCDATETIME()
            WHERE StatusCode = 'APPROVED'
              AND @FiscalPeriodID BETWEEN StartPeriodID AND EndPeriodID;
            
            INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, RowsAffected)
            VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                    DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()), 'COMPLETED', @@ROWCOUNT);
            
            COMMIT TRANSACTION LockPeriodTran;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION LockPeriodTran;
            
            INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
            VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                    DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()), 'FAILED', ERROR_MESSAGE());
            
            THROW;
        END CATCH
        
        -- Re-enable triggers
        EXEC('ENABLE TRIGGER ALL ON Planning.BudgetLineItem');
        
        -- =====================================================================
        -- Step 7: Send notifications
        -- =====================================================================
        IF @SendNotifications = 1 AND @NotificationRecipients IS NOT NULL
        BEGIN
            SET @CurrentStep = 'Send Notifications';
            SET @StepStartTime = SYSUTCDATETIME();
            
            DECLARE @EmailBody NVARCHAR(MAX);
            DECLARE @EmailSubject NVARCHAR(255);
            
            SET @EmailSubject = CONCAT('Financial Close Completed - ', @PeriodName, ' (', @FiscalYear, ')');
            
            -- Build HTML email body
            SET @EmailBody = CONCAT(
                N'<html><body>',
                N'<h2>Financial Close Summary</h2>',
                N'<p><strong>Period:</strong> ', @PeriodName, ' (', @FiscalYear, ')</p>',
                N'<p><strong>Close Type:</strong> ', @CloseType, '</p>',
                N'<p><strong>Completed:</strong> ', FORMAT(SYSUTCDATETIME(), 'yyyy-MM-dd HH:mm:ss'), ' UTC</p>',
                N'<h3>Processing Steps</h3>',
                N'<table border="1" cellpadding="5">',
                N'<tr><th>Step</th><th>Status</th><th>Duration (ms)</th><th>Rows</th></tr>'
            );
            
            SELECT @EmailBody = @EmailBody + CONCAT(
                N'<tr><td>', StepName, N'</td>',
                N'<td style="color:', 
                    CASE Status WHEN 'COMPLETED' THEN 'green' WHEN 'FAILED' THEN 'red' ELSE 'orange' END,
                    N'">', Status, N'</td>',
                N'<td>', DurationMs, N'</td>',
                N'<td>', ISNULL(CAST(RowsAffected AS VARCHAR), '-'), N'</td></tr>'
            )
            FROM @StepResults
            ORDER BY StepNumber;
            
            SET @EmailBody = @EmailBody + N'</table></body></html>';
            
            BEGIN TRY
                -- sp_send_dbmail - SQL Server Database Mail, no Snowflake equivalent
                EXEC msdb.dbo.sp_send_dbmail
                    @profile_name = 'FinanceNotifications',
                    @recipients = @NotificationRecipients,
                    @subject = @EmailSubject,
                    @body = @EmailBody,
                    @body_format = 'HTML';
                
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()), 'COMPLETED');
            END TRY
            BEGIN CATCH
                -- Email failure shouldn't fail the close
                INSERT INTO @StepResults (StepName, StartTime, EndTime, DurationMs, Status, ErrorMessage)
                VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 
                        DATEDIFF(MILLISECOND, @StepStartTime, SYSUTCDATETIME()), 'WARNING', ERROR_MESSAGE());
            END CATCH
        END
        
        SET @OverallStatus = 'COMPLETED';
        
    END TRY
    BEGIN CATCH
        SET @OverallStatus = 'FAILED';
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- Log final error
        INSERT INTO @StepResults (StepName, StartTime, EndTime, Status, ErrorMessage)
        VALUES ('ERROR_HANDLER', SYSUTCDATETIME(), SYSUTCDATETIME(), 'ERROR', @ErrorMessage);
    END CATCH
    
BuildResults:
    -- =========================================================================
    -- Build results XML
    -- =========================================================================
    SET @CloseResults = (
        SELECT 
            @CloseRunID AS '@RunID',
            @FiscalPeriodID AS '@PeriodID',
            @PeriodName AS '@PeriodName',
            @FiscalYear AS '@FiscalYear',
            @CloseType AS '@CloseType',
            @OverallStatus AS '@Status',
            DATEDIFF(MILLISECOND, @ProcStartTime, SYSUTCDATETIME()) AS '@TotalDurationMs',
            -- Validation errors
            (
                SELECT 
                    ErrorCode AS '@Code',
                    Severity AS '@Severity',
                    ErrorMessage AS 'text()'
                FROM @ValidationErrors
                FOR XML PATH('ValidationError'), TYPE
            ) AS ValidationErrors,
            -- Processing steps
            (
                SELECT 
                    StepNumber AS '@Sequence',
                    StepName AS '@Name',
                    Status AS '@Status',
                    DurationMs AS '@DurationMs',
                    RowsAffected AS RowsAffected,
                    ErrorMessage AS ErrorMessage,
                    OutputData AS OutputData
                FROM @StepResults
                ORDER BY StepNumber
                FOR XML PATH('Step'), TYPE
            ) AS ProcessingSteps,
            -- Summary metrics
            (
                SELECT 
                    (SELECT COUNT(*) FROM @StepResults WHERE Status = 'COMPLETED') AS CompletedSteps,
                    (SELECT COUNT(*) FROM @StepResults WHERE Status = 'FAILED') AS FailedSteps,
                    (SELECT COUNT(*) FROM @StepResults WHERE Status = 'WARNING') AS WarningSteps,
                    (SELECT SUM(DurationMs) FROM @StepResults) AS TotalProcessingMs,
                    (SELECT SUM(RowsAffected) FROM @StepResults) AS TotalRowsProcessed,
                    @ConsolidationBudgetID AS ConsolidatedBudgetID,
                    @UnreconciledCount AS UnreconciledItems,
                    @VarianceTotal AS TotalVariance
                FOR XML PATH('Metrics'), TYPE
            ) AS Summary
        FOR XML PATH('FinancialCloseResults')
    );
    
    -- Cleanup
    DROP TABLE IF EXISTS #CostCenterSnapshot;
    
    RETURN CASE @OverallStatus WHEN 'COMPLETED' THEN 0 WHEN 'VALIDATION_FAILED' THEN 1 ELSE -1 END;
END
GO
