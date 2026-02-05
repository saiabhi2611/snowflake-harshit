/*
    usp_ExecuteCostAllocation - Step-down cost allocation with recursive dependencies
    
    Dependencies:
        - Tables: AllocationRule, BudgetLineItem, CostCenter, GLAccount
        - Views: vw_AllocationRuleTargets
        - Functions: fn_GetAllocationFactor
        - Types: AllocationResultTableType
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. Recursive CTE with self-referencing updates
    2. WAITFOR DELAY for throttling - No Snowflake equivalent
    3. sp_getapplock/sp_releaseapplock for locking - Different concurrency model
    4. Table variable as OUTPUT parameter (via workaround)
    5. Nested procedure calls with OUTPUT parameters
    6. GOTO statements for control flow
    7. Multiple temp tables with complex joins
    8. CROSS APPLY to inline TVF with correlation
    9. TRY_CONVERT and TRY_CAST functions
    10. STRING_AGG with ordering
    11. UPDATE with FROM clause and TOP
    12. @@ROWCOUNT immediately after UPDATE in complex expressions
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_ExecuteCostAllocation
    @BudgetHeaderID         INT,
    @AllocationRuleIDs      VARCHAR(MAX) = NULL,  -- Comma-separated list, NULL = all active rules
    @FiscalPeriodID         INT = NULL,           -- NULL = all periods in budget
    @DryRun                 BIT = 0,
    @MaxIterations          INT = 100,
    @ThrottleDelayMS        INT = 0,              -- Delay between iterations
    @ConcurrencyMode        VARCHAR(20) = 'EXCLUSIVE',  -- EXCLUSIVE, SHARED, NONE
    @AllocationResults      Planning.AllocationResultTableType READONLY,  -- Input TVP
    @RowsAllocated          INT = NULL OUTPUT,
    @WarningMessages        NVARCHAR(MAX) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- =========================================================================
    -- Declarations
    -- =========================================================================
    DECLARE @ReturnCode INT = 0;
    DECLARE @IterationCount INT = 0;
    DECLARE @RowsThisIteration INT = 1;
    DECLARE @TotalAllocated DECIMAL(19,4) = 0;
    DECLARE @RemainingToAllocate DECIMAL(19,4);
    DECLARE @LockResult INT;
    DECLARE @WarningList NVARCHAR(MAX) = N'';
    
    -- Temp tables for processing
    CREATE TABLE #AllocationQueue (
        QueueID                 INT IDENTITY(1,1) PRIMARY KEY,
        AllocationRuleID        INT NOT NULL,
        SourceBudgetLineItemID  BIGINT NOT NULL,
        SourceAmount            DECIMAL(19,4) NOT NULL,
        RemainingAmount         DECIMAL(19,4) NOT NULL,
        ExecutionSequence       INT NOT NULL,
        DependsOnRuleID         INT NULL,
        IsProcessed             BIT DEFAULT 0,
        ProcessedDateTime       DATETIME2 NULL,
        ErrorMessage            NVARCHAR(500) NULL,
        INDEX IX_Sequence (ExecutionSequence, IsProcessed)
    );
    
    CREATE TABLE #AllocationResults (
        ResultID                INT IDENTITY(1,1) PRIMARY KEY,
        SourceBudgetLineItemID  BIGINT NOT NULL,
        TargetCostCenterID      INT NOT NULL,
        TargetGLAccountID       INT NOT NULL,
        AllocatedAmount         DECIMAL(19,4) NOT NULL,
        AllocationPercentage    DECIMAL(8,6) NOT NULL,
        AllocationRuleID        INT NOT NULL,
        IterationNumber         INT NOT NULL
    );
    
    CREATE TABLE #ProcessedRules (
        AllocationRuleID        INT PRIMARY KEY,
        ProcessedAt             DATETIME2,
        TotalAllocated          DECIMAL(19,4),
        TargetCount             INT
    );
    
    -- Table for rule dependencies (for step-down processing)
    CREATE TABLE #RuleDependencies (
        RuleID                  INT NOT NULL,
        DependsOnRuleID         INT NOT NULL,
        DependencyLevel         INT NOT NULL,
        PRIMARY KEY (RuleID, DependsOnRuleID)
    );
    
    -- =========================================================================
    -- Acquire application lock for exclusive processing
    -- =========================================================================
    IF @ConcurrencyMode = 'EXCLUSIVE'
    BEGIN
        EXEC @LockResult = sp_getapplock 
            @Resource = 'CostAllocation_Process',
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 30000;  -- 30 second timeout
        
        IF @LockResult < 0
        BEGIN
            SET @WarningMessages = 'Could not acquire exclusive lock. Another allocation process may be running.';
            SET @ReturnCode = -1;
            GOTO CleanupAndExit;
        END
    END
    ELSE IF @ConcurrencyMode = 'SHARED'
    BEGIN
        EXEC @LockResult = sp_getapplock 
            @Resource = 'CostAllocation_Process',
            @LockMode = 'Shared',
            @LockOwner = 'Session',
            @LockTimeout = 10000;
    END
    
    BEGIN TRY
        BEGIN TRANSACTION AllocationTran;
        
        -- =====================================================================
        -- Parse rule list and build dependency graph
        -- =====================================================================
        IF @AllocationRuleIDs IS NOT NULL
        BEGIN
            -- Use STRING_SPLIT (SQL Server 2016+) - requires refactoring for Snowflake
            INSERT INTO #RuleDependencies (RuleID, DependsOnRuleID, DependencyLevel)
            SELECT 
                ar.AllocationRuleID,
                ar.DependsOnRuleID,
                1
            FROM Planning.AllocationRule ar
            INNER JOIN STRING_SPLIT(@AllocationRuleIDs, ',') ss 
                ON ar.AllocationRuleID = TRY_CONVERT(INT, LTRIM(RTRIM(ss.value)))
            WHERE ar.IsActive = 1
              AND ar.DependsOnRuleID IS NOT NULL;
        END
        ELSE
        BEGIN
            -- All active rules
            INSERT INTO #RuleDependencies (RuleID, DependsOnRuleID, DependencyLevel)
            SELECT 
                ar.AllocationRuleID,
                ar.DependsOnRuleID,
                1
            FROM Planning.AllocationRule ar
            WHERE ar.IsActive = 1
              AND ar.DependsOnRuleID IS NOT NULL
              AND GETDATE() BETWEEN ar.EffectiveFromDate AND ISNULL(ar.EffectiveToDate, '9999-12-31');
        END
        
        -- Build transitive closure of dependencies using recursive CTE
        ;WITH RecursiveDeps AS (
            -- Anchor: direct dependencies
            SELECT 
                rd.RuleID,
                rd.DependsOnRuleID,
                1 AS Level,
                CAST(CAST(rd.RuleID AS VARCHAR) + '->' + CAST(rd.DependsOnRuleID AS VARCHAR) AS VARCHAR(MAX)) AS Path
            FROM #RuleDependencies rd
            
            UNION ALL
            
            -- Recursive: transitive dependencies
            SELECT 
                r.RuleID,
                rd.DependsOnRuleID,
                r.Level + 1,
                r.Path + '->' + CAST(rd.DependsOnRuleID AS VARCHAR)
            FROM RecursiveDeps r
            INNER JOIN #RuleDependencies rd ON r.DependsOnRuleID = rd.RuleID
            WHERE r.Level < 10  -- Prevent infinite recursion
              AND CHARINDEX(CAST(rd.DependsOnRuleID AS VARCHAR), r.Path) = 0  -- Cycle detection
        )
        INSERT INTO #RuleDependencies (RuleID, DependsOnRuleID, DependencyLevel)
        SELECT DISTINCT RuleID, DependsOnRuleID, MAX(Level)
        FROM RecursiveDeps
        WHERE NOT EXISTS (
            SELECT 1 FROM #RuleDependencies rd 
            WHERE rd.RuleID = RecursiveDeps.RuleID 
              AND rd.DependsOnRuleID = RecursiveDeps.DependsOnRuleID
        )
        GROUP BY RuleID, DependsOnRuleID
        OPTION (MAXRECURSION 100);
        
        -- =====================================================================
        -- Populate allocation queue using CROSS APPLY to inline TVF
        -- =====================================================================
        INSERT INTO #AllocationQueue (
            AllocationRuleID, SourceBudgetLineItemID, SourceAmount, 
            RemainingAmount, ExecutionSequence, DependsOnRuleID
        )
        SELECT 
            ar.AllocationRuleID,
            bli.BudgetLineItemID,
            bli.FinalAmount,
            bli.FinalAmount,
            ar.ExecutionSequence,
            ar.DependsOnRuleID
        FROM Planning.AllocationRule ar
        CROSS APPLY (
            -- Complex APPLY with pattern matching
            SELECT bli.*
            FROM Planning.BudgetLineItem bli
            INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
            INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
            WHERE bli.BudgetHeaderID = @BudgetHeaderID
              AND (@FiscalPeriodID IS NULL OR bli.FiscalPeriodID = @FiscalPeriodID)
              AND (ar.SourceCostCenterID IS NULL OR cc.CostCenterID = ar.SourceCostCenterID)
              -- Pattern matching using LIKE with escape
              AND (ar.SourceCostCenterPattern IS NULL 
                   OR cc.CostCenterCode LIKE ar.SourceCostCenterPattern ESCAPE '\')
              AND (ar.SourceAccountPattern IS NULL 
                   OR gla.AccountNumber LIKE ar.SourceAccountPattern ESCAPE '\')
              AND bli.FinalAmount <> 0
              AND bli.IsAllocated = 0
        ) bli
        WHERE ar.IsActive = 1
          AND (@AllocationRuleIDs IS NULL 
               OR ar.AllocationRuleID IN (
                   SELECT TRY_CONVERT(INT, LTRIM(RTRIM(value))) 
                   FROM STRING_SPLIT(@AllocationRuleIDs, ',')
               ))
        ORDER BY ar.ExecutionSequence, bli.BudgetLineItemID;
        
        -- =====================================================================
        -- Main allocation loop
        -- =====================================================================
        WHILE @RowsThisIteration > 0 AND @IterationCount < @MaxIterations
        BEGIN
            SET @IterationCount = @IterationCount + 1;
            SET @RowsThisIteration = 0;
            
            -- Throttle if requested - WAITFOR has no Snowflake equivalent
            IF @ThrottleDelayMS > 0 AND @IterationCount > 1
            BEGIN
                DECLARE @WaitTime VARCHAR(12) = CONVERT(VARCHAR(12), DATEADD(MILLISECOND, @ThrottleDelayMS, 0), 114);
                WAITFOR DELAY @WaitTime;
            END
            
            -- Process rules in sequence, respecting dependencies
            -- Use UPDATE with TOP to process in batches
            ;WITH OrderedQueue AS (
                SELECT TOP (1000) q.*
                FROM #AllocationQueue q
                WHERE q.IsProcessed = 0
                AND (q.DependsOnRuleID IS NULL 
                    OR EXISTS (SELECT 1 FROM #ProcessedRules pr WHERE pr.AllocationRuleID = q.DependsOnRuleID))
                ORDER BY q.ExecutionSequence, q.QueueID
            )
            UPDATE q
            SET 
                q.IsProcessed = 1,
                q.ProcessedDateTime = SYSUTCDATETIME()
            OUTPUT 
                deleted.SourceBudgetLineItemID,
                deleted.AllocationRuleID,
                deleted.SourceAmount,
                deleted.RemainingAmount
            INTO #AllocationResults (SourceBudgetLineItemID, AllocationRuleID, AllocatedAmount, AllocationPercentage)
            FROM OrderedQueue q;
            
            SET @RowsThisIteration = @@ROWCOUNT;
            
            -- Perform actual allocations for this batch
            IF @RowsThisIteration > 0
            BEGIN
                -- Complex allocation calculation using scalar function
                INSERT INTO #AllocationResults (
                    SourceBudgetLineItemID, TargetCostCenterID, TargetGLAccountID,
                    AllocatedAmount, AllocationPercentage, AllocationRuleID, IterationNumber
                )
                SELECT 
                    q.SourceBudgetLineItemID,
                    vt.TargetCostCenterID,
                    bli.GLAccountID,
                    CASE 
                        WHEN ar.RoundingMethod = 'UP' THEN CEILING(q.RemainingAmount * vt.TargetAllocationPct * 100) / 100
                        WHEN ar.RoundingMethod = 'DOWN' THEN FLOOR(q.RemainingAmount * vt.TargetAllocationPct * 100) / 100
                        ELSE ROUND(q.RemainingAmount * 
                             Planning.fn_GetAllocationFactor(
                                 bli.CostCenterID, 
                                 vt.TargetCostCenterID, 
                                 ar.AllocationBasis, 
                                 bli.FiscalPeriodID, 
                                 @BudgetHeaderID
                             ), 2)
                    END,
                    ISNULL(vt.TargetAllocationPct, 
                           Planning.fn_GetAllocationFactor(
                               bli.CostCenterID, 
                               vt.TargetCostCenterID, 
                               ar.AllocationBasis, 
                               bli.FiscalPeriodID, 
                               @BudgetHeaderID
                           )),
                    q.AllocationRuleID,
                    @IterationCount
                FROM #AllocationQueue q
                INNER JOIN Planning.AllocationRule ar ON q.AllocationRuleID = ar.AllocationRuleID
                INNER JOIN Planning.BudgetLineItem bli ON q.SourceBudgetLineItemID = bli.BudgetLineItemID
                CROSS APPLY (
                    SELECT * FROM Planning.vw_AllocationRuleTargets vt
                    WHERE vt.AllocationRuleID = ar.AllocationRuleID
                      AND vt.TargetIsActive = 1
                ) vt
                WHERE q.ProcessedDateTime IS NOT NULL
                  AND q.ProcessedDateTime >= DATEADD(SECOND, -5, SYSUTCDATETIME());
                
                -- Track processed rules
                MERGE INTO #ProcessedRules AS target
                USING (
                    SELECT 
                        AllocationRuleID,
                        SYSUTCDATETIME() AS ProcessedAt,
                        SUM(AllocatedAmount) AS TotalAllocated,
                        COUNT(*) AS TargetCount
                    FROM #AllocationResults
                    WHERE IterationNumber = @IterationCount
                    GROUP BY AllocationRuleID
                ) AS source
                ON target.AllocationRuleID = source.AllocationRuleID
                WHEN MATCHED THEN
                    UPDATE SET 
                        TotalAllocated = target.TotalAllocated + source.TotalAllocated,
                        TargetCount = target.TargetCount + source.TargetCount
                WHEN NOT MATCHED THEN
                    INSERT (AllocationRuleID, ProcessedAt, TotalAllocated, TargetCount)
                    VALUES (source.AllocationRuleID, source.ProcessedAt, source.TotalAllocated, source.TargetCount);
            END
        END
        
        -- =====================================================================
        -- Persist results (unless dry run)
        -- =====================================================================
        IF @DryRun = 0
        BEGIN
            -- Insert allocated line items
            INSERT INTO Planning.BudgetLineItem (
                BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                OriginalAmount, AdjustedAmount, IsAllocated, AllocationSourceLineID,
                AllocationPercentage, LastModifiedDateTime
            )
            SELECT 
                @BudgetHeaderID,
                ar.TargetGLAccountID,
                ar.TargetCostCenterID,
                bli.FiscalPeriodID,
                ar.AllocatedAmount,
                0,
                1,
                ar.SourceBudgetLineItemID,
                ar.AllocationPercentage,
                SYSUTCDATETIME()
            FROM #AllocationResults ar
            INNER JOIN Planning.BudgetLineItem bli ON ar.SourceBudgetLineItemID = bli.BudgetLineItemID;
            
            SET @RowsAllocated = @@ROWCOUNT;
            
            -- Mark source items as allocated
            UPDATE bli
            SET IsAllocated = 1
            FROM Planning.BudgetLineItem bli
            INNER JOIN (
                SELECT DISTINCT SourceBudgetLineItemID 
                FROM #AllocationResults
            ) ar ON bli.BudgetLineItemID = ar.SourceBudgetLineItemID;
        END
        ELSE
        BEGIN
            -- Dry run - just return results
            SET @RowsAllocated = (SELECT COUNT(*) FROM #AllocationResults);
        END
        
        -- =====================================================================
        -- Build warning messages using STRING_AGG
        -- =====================================================================
        SET @WarningMessages = (
            SELECT STRING_AGG(
                CONCAT('Rule ', AllocationRuleID, ': ', ErrorMessage),
                '; '
            ) WITHIN GROUP (ORDER BY QueueID)  -- STRING_AGG with ORDER BY
            FROM #AllocationQueue
            WHERE ErrorMessage IS NOT NULL
        );
        
        -- Add iteration warning if max reached
        IF @IterationCount >= @MaxIterations
        BEGIN
            SET @WarningMessages = CONCAT(
                @WarningMessages, 
                '; WARNING: Max iterations (', @MaxIterations, ') reached. Some allocations may be incomplete.'
            );
        END
        
        COMMIT TRANSACTION AllocationTran;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION AllocationTran;
        
        SET @WarningMessages = ERROR_MESSAGE();
        SET @ReturnCode = ERROR_NUMBER();
    END CATCH
    
CleanupAndExit:
    -- Release application lock
    IF @ConcurrencyMode IN ('EXCLUSIVE', 'SHARED')
    BEGIN
        EXEC sp_releaseapplock 
            @Resource = 'CostAllocation_Process',
            @LockOwner = 'Session';
    END
    
    -- Cleanup temp tables
    DROP TABLE IF EXISTS #AllocationQueue;
    DROP TABLE IF EXISTS #AllocationResults;
    DROP TABLE IF EXISTS #ProcessedRules;
    DROP TABLE IF EXISTS #RuleDependencies;
    
    RETURN @ReturnCode;
END
GO
