/*
    usp_ProcessBudgetConsolidation - Complex budget consolidation with hierarchy rollup
    
    Dependencies: 
        - Tables: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod, ConsolidationJournal
        - Views: vw_BudgetConsolidationSummary
        - Functions: fn_GetHierarchyPath, tvf_ExplodeCostCenterHierarchy
        - Types: BudgetLineItemTableType, AllocationResultTableType
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. CURSOR with FAST_FORWARD and SCROLL options - Snowflake has no cursor support
    2. Table variables with indexes - No equivalent
    3. WHILE loops with complex break conditions
    4. Nested transactions with named savepoints - Limited in Snowflake
    5. TRY-CATCH with THROW/RAISERROR - Different exception model
    6. SCOPE_IDENTITY() after inserts
    7. OUTPUT clause capturing inserted rows
    8. Cross-apply with table-valued function
    9. Dynamic SQL with sp_executesql and output parameters
    10. MERGE with complex matching and OUTPUT
    11. @@TRANCOUNT and transaction nesting
    12. SET XACT_ABORT, NOCOUNT patterns
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_ProcessBudgetConsolidation
    @SourceBudgetHeaderID       INT,
    @TargetBudgetHeaderID       INT = NULL OUTPUT,
    @ConsolidationType          VARCHAR(20) = 'FULL',         -- FULL, INCREMENTAL, DELTA
    @IncludeEliminations        BIT = 1,
    @RecalculateAllocations     BIT = 1,
    @ProcessingOptions          XML = NULL,
    @UserID                     INT = NULL,
    @DebugMode                  BIT = 0,
    @RowsProcessed              INT = NULL OUTPUT,
    @ErrorMessage               NVARCHAR(4000) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;  -- We'll handle errors manually
    
    -- =========================================================================
    -- Variable declarations
    -- =========================================================================
    DECLARE @ProcStartTime DATETIME2 = SYSUTCDATETIME();
    DECLARE @StepStartTime DATETIME2;
    DECLARE @CurrentStep NVARCHAR(100);
    DECLARE @ReturnCode INT = 0;
    DECLARE @TotalRowsProcessed INT = 0;
    DECLARE @BatchSize INT = 5000;
    DECLARE @CurrentBatch INT = 0;
    DECLARE @MaxIterations INT = 1000;
    DECLARE @ConsolidationRunID UNIQUEIDENTIFIER = NEWID();
    
    -- Table variables - These don't exist in Snowflake
    DECLARE @ProcessingLog TABLE (
        LogID               INT IDENTITY(1,1) PRIMARY KEY,
        StepName            NVARCHAR(100),
        StartTime           DATETIME2,
        EndTime             DATETIME2,
        RowsAffected        INT,
        StatusCode          VARCHAR(20),
        Message             NVARCHAR(MAX),
        INDEX IX_StepName (StepName)
    );
    
    DECLARE @HierarchyNodes TABLE (
        NodeID              INT PRIMARY KEY,
        ParentNodeID        INT,
        NodeLevel           INT,
        ProcessingOrder     INT,
        IsProcessed         BIT DEFAULT 0,
        SubtotalAmount      DECIMAL(19,4),
        INDEX IX_Level (NodeLevel, IsProcessed)
    );
    
    DECLARE @ConsolidatedAmounts TABLE (
        GLAccountID         INT NOT NULL,
        CostCenterID        INT NOT NULL,
        FiscalPeriodID      INT NOT NULL,
        ConsolidatedAmount  DECIMAL(19,4) NOT NULL,
        EliminationAmount   DECIMAL(19,4) DEFAULT 0,
        FinalAmount         DECIMAL(19,4),
        SourceCount         INT,
        PRIMARY KEY (GLAccountID, CostCenterID, FiscalPeriodID)
    );
    
    -- =========================================================================
    -- Cursor declarations - No Snowflake equivalent
    -- =========================================================================
    DECLARE @CursorCostCenterID INT;
    DECLARE @CursorLevel INT;
    DECLARE @CursorParentID INT;
    DECLARE @CursorSubtotal DECIMAL(19,4);
    
    DECLARE HierarchyCursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
        SELECT NodeID, NodeLevel, ParentNodeID
        FROM @HierarchyNodes
        ORDER BY NodeLevel DESC, NodeID;  -- Process bottom-up
    
    -- Secondary cursor for elimination entries
    DECLARE @ElimAccountID INT;
    DECLARE @ElimCostCenterID INT;
    DECLARE @ElimAmount DECIMAL(19,4);
    DECLARE @PartnerEntityCode VARCHAR(20);
    
    DECLARE EliminationCursor CURSOR LOCAL SCROLL KEYSET FOR
        SELECT 
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FinalAmount,
            gla.StatutoryAccountCode  -- Uses SPARSE column
        FROM Planning.BudgetLineItem bli
        INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
        WHERE bli.BudgetHeaderID = @SourceBudgetHeaderID
          AND gla.IntercompanyFlag = 1
        ORDER BY bli.GLAccountID, bli.CostCenterID
        FOR UPDATE OF bli.AdjustedAmount;  -- Updateable cursor
    
    -- =========================================================================
    -- TRY-CATCH Error Handling - Different in Snowflake
    -- =========================================================================
    BEGIN TRY
        -- Validate input parameters
        SET @CurrentStep = 'Parameter Validation';
        SET @StepStartTime = SYSUTCDATETIME();
        
        IF NOT EXISTS (SELECT 1 FROM Planning.BudgetHeader WHERE BudgetHeaderID = @SourceBudgetHeaderID)
        BEGIN
            SET @ErrorMessage = 'Source budget header not found: ' + CAST(@SourceBudgetHeaderID AS VARCHAR);
            RAISERROR(@ErrorMessage, 16, 1);
        END
        
        -- Check if source is locked
        IF EXISTS (
            SELECT 1 FROM Planning.BudgetHeader 
            WHERE BudgetHeaderID = @SourceBudgetHeaderID 
              AND StatusCode NOT IN ('APPROVED', 'LOCKED')
        )
        BEGIN
            SET @ErrorMessage = 'Source budget must be in APPROVED or LOCKED status for consolidation';
            THROW 50001, @ErrorMessage, 1;  -- THROW syntax differs from RAISERROR
        END
        
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 0, 'COMPLETED');
        
        -- =====================================================================
        -- Create or update target budget header
        -- =====================================================================
        SET @CurrentStep = 'Create Target Budget';
        SET @StepStartTime = SYSUTCDATETIME();
        
        BEGIN TRANSACTION ConsolidationTran;
        
        IF @TargetBudgetHeaderID IS NULL
        BEGIN
            -- Create new consolidated budget header using OUTPUT clause
            DECLARE @InsertedHeaders TABLE (BudgetHeaderID INT, BudgetCode VARCHAR(30));
            
            INSERT INTO Planning.BudgetHeader (
                BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear,
                StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode,
                VersionNumber, ExtendedProperties
            )
            OUTPUT inserted.BudgetHeaderID, inserted.BudgetCode INTO @InsertedHeaders
            SELECT 
                BudgetCode + '_CONSOL_' + FORMAT(GETDATE(), 'yyyyMMdd'),
                BudgetName + ' - Consolidated',
                'CONSOLIDATED',
                ScenarioType,
                FiscalYear,
                StartPeriodID,
                EndPeriodID,
                BudgetHeaderID,
                'DRAFT',
                1,
                -- XML modification - very different in Snowflake
                CAST(
                    '<Root>' +
                    '<ConsolidationRun RunID="' + CAST(@ConsolidationRunID AS VARCHAR(36)) + '" ' +
                    'SourceID="' + CAST(@SourceBudgetHeaderID AS VARCHAR(20)) + '" ' +
                    'Timestamp="' + CONVERT(VARCHAR(30), @ProcStartTime, 126) + '"/>' +
                    ISNULL(CAST(ExtendedProperties AS NVARCHAR(MAX)), '') +
                    '</Root>' AS XML
                )
            FROM Planning.BudgetHeader
            WHERE BudgetHeaderID = @SourceBudgetHeaderID;
            
            SELECT @TargetBudgetHeaderID = BudgetHeaderID FROM @InsertedHeaders;
            
            IF @TargetBudgetHeaderID IS NULL
            BEGIN
                SET @ErrorMessage = 'Failed to create target budget header';
                THROW 50002, @ErrorMessage, 1;
            END
        END
        
        -- Savepoint for partial rollback - Limited Snowflake support
        SAVE TRANSACTION SavePoint_AfterHeader;
        
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 1, 'COMPLETED');
        
        -- =====================================================================
        -- Build hierarchy for bottom-up rollup using TVF
        -- =====================================================================
        SET @CurrentStep = 'Build Hierarchy';
        SET @StepStartTime = SYSUTCDATETIME();
        
        INSERT INTO @HierarchyNodes (NodeID, ParentNodeID, NodeLevel, ProcessingOrder)
        SELECT 
            h.CostCenterID,
            h.ParentCostCenterID,
            h.HierarchyLevel,
            ROW_NUMBER() OVER (ORDER BY h.HierarchyLevel DESC, h.CostCenterID)
        FROM Planning.tvf_ExplodeCostCenterHierarchy(NULL, 10, 0, GETDATE()) h;  -- CROSS APPLY to TVF
        
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), @@ROWCOUNT, 'COMPLETED');
        
        -- =====================================================================
        -- Process consolidation using cursor (bottom-up hierarchy traversal)
        -- =====================================================================
        SET @CurrentStep = 'Hierarchy Consolidation';
        SET @StepStartTime = SYSUTCDATETIME();
        
        OPEN HierarchyCursor;
        
        FETCH NEXT FROM HierarchyCursor INTO @CursorCostCenterID, @CursorLevel, @CursorParentID;
        
        WHILE @@FETCH_STATUS = 0 AND @CurrentBatch < @MaxIterations
        BEGIN
            SET @CurrentBatch = @CurrentBatch + 1;
            
            -- Calculate subtotal for this node
            SELECT @CursorSubtotal = SUM(bli.FinalAmount)
            FROM Planning.BudgetLineItem bli
            WHERE bli.BudgetHeaderID = @SourceBudgetHeaderID
              AND bli.CostCenterID = @CursorCostCenterID;
            
            -- Add child subtotals (already processed due to bottom-up order)
            SELECT @CursorSubtotal = ISNULL(@CursorSubtotal, 0) + ISNULL(SUM(h.SubtotalAmount), 0)
            FROM @HierarchyNodes h
            WHERE h.ParentNodeID = @CursorCostCenterID
              AND h.IsProcessed = 1;
            
            -- Update node
            UPDATE @HierarchyNodes
            SET SubtotalAmount = @CursorSubtotal,
                IsProcessed = 1
            WHERE NodeID = @CursorCostCenterID;
            
            -- MERGE to update or insert consolidated amounts
            MERGE INTO @ConsolidatedAmounts AS target
            USING (
                SELECT 
                    bli.GLAccountID,
                    @CursorCostCenterID AS CostCenterID,
                    bli.FiscalPeriodID,
                    SUM(bli.FinalAmount) AS Amount,
                    COUNT(*) AS SourceCnt
                FROM Planning.BudgetLineItem bli
                WHERE bli.BudgetHeaderID = @SourceBudgetHeaderID
                  AND bli.CostCenterID = @CursorCostCenterID
                GROUP BY bli.GLAccountID, bli.FiscalPeriodID
            ) AS source
            ON target.GLAccountID = source.GLAccountID
               AND target.CostCenterID = source.CostCenterID
               AND target.FiscalPeriodID = source.FiscalPeriodID
            WHEN MATCHED THEN
                UPDATE SET 
                    ConsolidatedAmount = target.ConsolidatedAmount + source.Amount,
                    SourceCount = target.SourceCount + source.SourceCnt
            WHEN NOT MATCHED THEN
                INSERT (GLAccountID, CostCenterID, FiscalPeriodID, ConsolidatedAmount, SourceCount)
                VALUES (source.GLAccountID, source.CostCenterID, source.FiscalPeriodID, source.Amount, source.SourceCnt);
            
            SET @TotalRowsProcessed = @TotalRowsProcessed + @@ROWCOUNT;
            
            FETCH NEXT FROM HierarchyCursor INTO @CursorCostCenterID, @CursorLevel, @CursorParentID;
        END
        
        CLOSE HierarchyCursor;
        DEALLOCATE HierarchyCursor;
        
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), @TotalRowsProcessed, 'COMPLETED');
        
        -- =====================================================================
        -- Process intercompany eliminations using updateable cursor
        -- =====================================================================
        IF @IncludeEliminations = 1
        BEGIN
            SET @CurrentStep = 'Intercompany Eliminations';
            SET @StepStartTime = SYSUTCDATETIME();
            DECLARE @EliminationCount INT = 0;
            
            -- Savepoint before eliminations
            SAVE TRANSACTION SavePoint_BeforeEliminations;
            
            OPEN EliminationCursor;
            
            FETCH NEXT FROM EliminationCursor 
            INTO @ElimAccountID, @ElimCostCenterID, @ElimAmount, @PartnerEntityCode;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Complex elimination logic with scrollable cursor
                IF @ElimAmount <> 0
                BEGIN
                    -- Check for matching offsetting entry
                    DECLARE @OffsetExists BIT = 0;
                    DECLARE @OffsetAmount DECIMAL(19,4);
                    
                    -- Use cursor positioning to look for offset
                    FETCH RELATIVE 1 FROM EliminationCursor 
                    INTO @ElimAccountID, @ElimCostCenterID, @OffsetAmount, @PartnerEntityCode;
                    
                    IF @@FETCH_STATUS = 0 AND @OffsetAmount = -@ElimAmount
                    BEGIN
                        SET @OffsetExists = 1;
                        
                        -- Create elimination entry
                        UPDATE @ConsolidatedAmounts
                        SET EliminationAmount = EliminationAmount + @ElimAmount
                        WHERE GLAccountID = @ElimAccountID
                          AND CostCenterID = @ElimCostCenterID;
                        
                        SET @EliminationCount = @EliminationCount + 1;
                    END
                    
                    -- Move back if no offset found
                    IF @OffsetExists = 0
                        FETCH PRIOR FROM EliminationCursor 
                        INTO @ElimAccountID, @ElimCostCenterID, @ElimAmount, @PartnerEntityCode;
                END
                
                FETCH NEXT FROM EliminationCursor 
                INTO @ElimAccountID, @ElimCostCenterID, @ElimAmount, @PartnerEntityCode;
            END
            
            CLOSE EliminationCursor;
            DEALLOCATE EliminationCursor;
            
            INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), @EliminationCount, 'COMPLETED');
        END
        
        -- =====================================================================
        -- Recalculate allocations using dynamic SQL
        -- =====================================================================
        IF @RecalculateAllocations = 1
        BEGIN
            SET @CurrentStep = 'Recalculate Allocations';
            SET @StepStartTime = SYSUTCDATETIME();
            
            DECLARE @DynamicSQL NVARCHAR(MAX);
            DECLARE @ParamDefinition NVARCHAR(500);
            DECLARE @AllocationRowCount INT;
            
            -- Build dynamic SQL based on processing options
            SET @DynamicSQL = N'
                UPDATE ca
                SET FinalAmount = ca.ConsolidatedAmount - ca.EliminationAmount
                FROM @ConsolidatedAmounts ca
                WHERE ca.ConsolidatedAmount <> 0
                  OR ca.EliminationAmount <> 0;
                
                SET @RowCountOUT = @@ROWCOUNT;
            ';
            
            -- Extract options from XML if provided
            IF @ProcessingOptions IS NOT NULL
            BEGIN
                DECLARE @IncludeZeroBalances BIT;
                DECLARE @RoundingPrecision INT;
                
                SELECT 
                    @IncludeZeroBalances = @ProcessingOptions.value('(/Options/IncludeZeroBalances)[1]', 'BIT'),
                    @RoundingPrecision = @ProcessingOptions.value('(/Options/RoundingPrecision)[1]', 'INT');
                
                -- Modify SQL based on options
                IF @IncludeZeroBalances = 0
                    SET @DynamicSQL = REPLACE(@DynamicSQL, 
                        'WHERE ca.ConsolidatedAmount <> 0',
                        'WHERE ca.ConsolidatedAmount <> 0 AND ca.FinalAmount <> 0');
                
                IF @RoundingPrecision IS NOT NULL
                    SET @DynamicSQL = REPLACE(@DynamicSQL,
                        'ca.ConsolidatedAmount - ca.EliminationAmount',
                        'ROUND(ca.ConsolidatedAmount - ca.EliminationAmount, ' + CAST(@RoundingPrecision AS VARCHAR) + ')');
            END
            
            SET @ParamDefinition = N'@RowCountOUT INT OUTPUT';
            
            -- This pattern with table variables in dynamic SQL is very SQL Server-specific
            EXEC sp_executesql @DynamicSQL, @ParamDefinition, @RowCountOUT = @AllocationRowCount OUTPUT;
            
            INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), @AllocationRowCount, 'COMPLETED');
        END
        
        -- =====================================================================
        -- Insert final results with OUTPUT clause
        -- =====================================================================
        SET @CurrentStep = 'Insert Results';
        SET @StepStartTime = SYSUTCDATETIME();
        
        DECLARE @InsertedLines TABLE (
            BudgetLineItemID BIGINT,
            GLAccountID INT,
            CostCenterID INT,
            Amount DECIMAL(19,4)
        );
        
        INSERT INTO Planning.BudgetLineItem (
            BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem, SourceReference,
            IsAllocated, LastModifiedByUserID, LastModifiedDateTime
        )
        OUTPUT 
            inserted.BudgetLineItemID,
            inserted.GLAccountID,
            inserted.CostCenterID,
            inserted.OriginalAmount
        INTO @InsertedLines
        SELECT 
            @TargetBudgetHeaderID,
            ca.GLAccountID,
            ca.CostCenterID,
            ca.FiscalPeriodID,
            ca.FinalAmount,
            0,
            'CONSOLIDATED',
            'CONSOLIDATION_PROC',
            CAST(@ConsolidationRunID AS VARCHAR(50)),
            0,
            @UserID,
            SYSUTCDATETIME()
        FROM @ConsolidatedAmounts ca
        WHERE ca.FinalAmount IS NOT NULL;
        
        SET @TotalRowsProcessed = @TotalRowsProcessed + @@ROWCOUNT;
        
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), @@ROWCOUNT, 'COMPLETED');
        
        -- =====================================================================
        -- Commit transaction
        -- =====================================================================
        IF @@TRANCOUNT > 0
            COMMIT TRANSACTION ConsolidationTran;
        
        SET @RowsProcessed = @TotalRowsProcessed;
        
        -- Debug output
        IF @DebugMode = 1
        BEGIN
            SELECT * FROM @ProcessingLog ORDER BY LogID;
            SELECT * FROM @InsertedLines;
        END
        
    END TRY
    BEGIN CATCH
        -- =====================================================================
        -- Error handling block - Pattern differs significantly in Snowflake
        -- =====================================================================
        SET @ReturnCode = ERROR_NUMBER();
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- Check transaction state and rollback appropriately
        IF @@TRANCOUNT > 0
        BEGIN
            -- Try to rollback to savepoint first if possible
            IF XACT_STATE() = 1
            BEGIN
                ROLLBACK TRANSACTION SavePoint_AfterHeader;
            END
            ELSE
            BEGIN
                ROLLBACK TRANSACTION ConsolidationTran;
            END
        END
        
        -- Cleanup cursors if open
        IF CURSOR_STATUS('local', 'HierarchyCursor') >= 0
        BEGIN
            CLOSE HierarchyCursor;
            DEALLOCATE HierarchyCursor;
        END
        
        IF CURSOR_STATUS('local', 'EliminationCursor') >= 0
        BEGIN
            CLOSE EliminationCursor;
            DEALLOCATE EliminationCursor;
        END
        
        -- Log the error
        INSERT INTO @ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode, Message)
        VALUES (@CurrentStep, @StepStartTime, SYSUTCDATETIME(), 0, 'ERROR', @ErrorMessage);
        
        -- Re-throw the error
        THROW;
    END CATCH
    
    RETURN @ReturnCode;
END
GO
