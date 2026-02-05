/*
    usp_ReconcileIntercompanyBalances - Complex intercompany reconciliation with XML reporting
    
    Dependencies:
        - Tables: BudgetLineItem, ConsolidationJournal, ConsolidationJournalLine, GLAccount, CostCenter
        - Views: vw_BudgetConsolidationSummary
        - Functions: fn_GetHierarchyPath
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. OPENXML and XML DOM operations - Very different in Snowflake
    2. sp_xml_preparedocument / sp_xml_removedocument - No equivalent
    3. FOR XML PATH with complex nesting and attributes
    4. XML namespaces and xpath queries
    5. HASHBYTES for data comparison - Different syntax
    6. Binary data manipulation with CAST to VARBINARY
    7. CLR function references (commented out example)
    8. EVENTDATA() for DDL trigger context
    9. sys.dm_* dynamic management views
    10. DBCC commands embedded in procedures
    11. Extended stored procedures (xp_*)
    12. Linked server queries with OPENQUERY
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_ReconcileIntercompanyBalances
    @BudgetHeaderID             INT,
    @ReconciliationDate         DATE = NULL,
    @EntityCodes                XML = NULL,         -- List of entities to reconcile
    @ToleranceAmount            DECIMAL(19,4) = 0.01,
    @TolerancePercent           DECIMAL(5,4) = 0.001,
    @AutoCreateAdjustments      BIT = 0,
    @ReconciliationReportXML    XML = NULL OUTPUT,
    @UnreconciledCount          INT = NULL OUTPUT,
    @TotalVarianceAmount        DECIMAL(19,4) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @EffectiveDate DATE = ISNULL(@ReconciliationDate, CAST(GETDATE() AS DATE));
    DECLARE @ReconciliationID UNIQUEIDENTIFIER = NEWID();
    DECLARE @XMLHandle INT;
    DECLARE @ErrorMsg NVARCHAR(4000);
    
    -- Tables for reconciliation processing
    CREATE TABLE #IntercompanyPairs (
        PairID              INT IDENTITY(1,1) PRIMARY KEY,
        Entity1Code         VARCHAR(20) NOT NULL,
        Entity2Code         VARCHAR(20) NOT NULL,
        GLAccountID         INT NOT NULL,
        PartnerAccountID    INT NOT NULL,
        Entity1Amount       DECIMAL(19,4) NOT NULL,
        Entity2Amount       DECIMAL(19,4) NOT NULL,
        Variance            DECIMAL(19,4) NOT NULL,
        VariancePercent     DECIMAL(8,6) NULL,
        IsWithinTolerance   BIT NOT NULL,
        ReconciliationStatus VARCHAR(20),
        MatchHash           VARBINARY(32),
        INDEX IX_Entities (Entity1Code, Entity2Code),
        INDEX IX_Status (IsWithinTolerance, ReconciliationStatus)
    );
    
    CREATE TABLE #ReconciliationDetails (
        DetailID            INT IDENTITY(1,1) PRIMARY KEY,
        PairID              INT,
        SourceLineItemID    BIGINT,
        TargetLineItemID    BIGINT,
        MatchType           VARCHAR(20),  -- EXACT, PARTIAL, UNMATCHED
        MatchScore          DECIMAL(5,4),
        MatchDetails        NVARCHAR(500)
    );
    
    BEGIN TRY
        -- =====================================================================
        -- Parse entity list from XML using OPENXML (legacy pattern)
        -- =====================================================================
        DECLARE @EntityList TABLE (
            EntityCode      VARCHAR(20) PRIMARY KEY,
            EntityName      NVARCHAR(100),
            IncludeFlag     BIT DEFAULT 1
        );
        
        IF @EntityCodes IS NOT NULL
        BEGIN
            -- Prepare XML document handle
            EXEC sp_xml_preparedocument @XMLHandle OUTPUT, @EntityCodes;
            
            INSERT INTO @EntityList (EntityCode, EntityName, IncludeFlag)
            SELECT 
                EntityCode,
                EntityName,
                ISNULL(Include, 1)
            FROM OPENXML(@XMLHandle, '/Entities/Entity', 2)
            WITH (
                EntityCode  VARCHAR(20)     '@Code',
                EntityName  NVARCHAR(100)   '@Name',
                Include     BIT             '@Include'
            );
            
            -- Release XML document
            EXEC sp_xml_removedocument @XMLHandle;
        END
        ELSE
        BEGIN
            -- Get all distinct entities from budget data
            INSERT INTO @EntityList (EntityCode)
            SELECT DISTINCT 
                LEFT(cc.CostCenterCode, CHARINDEX('-', cc.CostCenterCode + '-') - 1)
            FROM Planning.BudgetLineItem bli
            INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
            WHERE bli.BudgetHeaderID = @BudgetHeaderID;
        END
        
        -- =====================================================================
        -- Identify intercompany pairs and calculate variances
        -- =====================================================================
        INSERT INTO #IntercompanyPairs (
            Entity1Code, Entity2Code, GLAccountID, PartnerAccountID,
            Entity1Amount, Entity2Amount, Variance, VariancePercent,
            IsWithinTolerance, ReconciliationStatus, MatchHash
        )
        SELECT 
            e1.EntityCode,
            e2.EntityCode,
            bli1.GLAccountID,
            gla1.ConsolidationAccountID,
            SUM(bli1.FinalAmount),
            -SUM(ISNULL(bli2.FinalAmount, 0)),  -- Opposite sign
            SUM(bli1.FinalAmount) + SUM(ISNULL(bli2.FinalAmount, 0)),
            CASE 
                WHEN ABS(SUM(bli1.FinalAmount)) > 0 
                THEN (SUM(bli1.FinalAmount) + SUM(ISNULL(bli2.FinalAmount, 0))) / ABS(SUM(bli1.FinalAmount))
                ELSE NULL 
            END,
            CASE 
                WHEN ABS(SUM(bli1.FinalAmount) + SUM(ISNULL(bli2.FinalAmount, 0))) <= @ToleranceAmount THEN 1
                WHEN ABS(SUM(bli1.FinalAmount)) > 0 
                     AND ABS((SUM(bli1.FinalAmount) + SUM(ISNULL(bli2.FinalAmount, 0))) / SUM(bli1.FinalAmount)) <= @TolerancePercent THEN 1
                ELSE 0
            END,
            'PENDING',
            -- HASHBYTES for matching - Different in Snowflake
            HASHBYTES('SHA2_256', 
                CONCAT(e1.EntityCode, '|', e2.EntityCode, '|', 
                       CAST(bli1.GLAccountID AS VARCHAR), '|',
                       CAST(ABS(ROUND(SUM(bli1.FinalAmount), 0)) AS VARCHAR)))
        FROM Planning.BudgetLineItem bli1
        INNER JOIN Planning.GLAccount gla1 ON bli1.GLAccountID = gla1.GLAccountID
        INNER JOIN Planning.CostCenter cc1 ON bli1.CostCenterID = cc1.CostCenterID
        CROSS APPLY (
            SELECT LEFT(cc1.CostCenterCode, CHARINDEX('-', cc1.CostCenterCode + '-') - 1) AS EntityCode
        ) e1
        INNER JOIN @EntityList el1 ON e1.EntityCode = el1.EntityCode AND el1.IncludeFlag = 1
        -- Find partner entries
        LEFT JOIN Planning.BudgetLineItem bli2 
            ON bli2.BudgetHeaderID = @BudgetHeaderID
            AND bli2.GLAccountID = gla1.ConsolidationAccountID
        LEFT JOIN Planning.CostCenter cc2 ON bli2.CostCenterID = cc2.CostCenterID
        CROSS APPLY (
            SELECT LEFT(ISNULL(cc2.CostCenterCode, ''), CHARINDEX('-', ISNULL(cc2.CostCenterCode, '') + '-') - 1) AS EntityCode
        ) e2
        LEFT JOIN @EntityList el2 ON e2.EntityCode = el2.EntityCode
        WHERE bli1.BudgetHeaderID = @BudgetHeaderID
          AND gla1.IntercompanyFlag = 1
          AND gla1.ConsolidationAccountID IS NOT NULL
        GROUP BY 
            e1.EntityCode, e2.EntityCode, 
            bli1.GLAccountID, gla1.ConsolidationAccountID
        HAVING SUM(bli1.FinalAmount) <> 0 OR SUM(ISNULL(bli2.FinalAmount, 0)) <> 0;
        
        -- =====================================================================
        -- Perform detailed matching using fuzzy logic
        -- =====================================================================
        INSERT INTO #ReconciliationDetails (
            PairID, SourceLineItemID, TargetLineItemID, 
            MatchType, MatchScore, MatchDetails
        )
        SELECT 
            ip.PairID,
            bli1.BudgetLineItemID,
            bli2.BudgetLineItemID,
            CASE 
                WHEN bli1.FinalAmount = -bli2.FinalAmount THEN 'EXACT'
                WHEN ABS(bli1.FinalAmount + bli2.FinalAmount) <= @ToleranceAmount THEN 'TOLERANCE'
                WHEN bli2.BudgetLineItemID IS NULL THEN 'UNMATCHED_SOURCE'
                ELSE 'PARTIAL'
            END,
            CASE 
                WHEN bli1.FinalAmount = -bli2.FinalAmount THEN 1.0
                WHEN ABS(bli1.FinalAmount) > 0 
                THEN 1.0 - ABS((bli1.FinalAmount + ISNULL(bli2.FinalAmount, 0)) / bli1.FinalAmount)
                ELSE 0
            END,
            CONCAT(
                'Source: ', FORMAT(bli1.FinalAmount, 'N2'),
                ' | Target: ', FORMAT(ISNULL(bli2.FinalAmount, 0), 'N2'),
                ' | Diff: ', FORMAT(bli1.FinalAmount + ISNULL(bli2.FinalAmount, 0), 'N2')
            )
        FROM #IntercompanyPairs ip
        INNER JOIN Planning.BudgetLineItem bli1 
            ON bli1.BudgetHeaderID = @BudgetHeaderID
            AND bli1.GLAccountID = ip.GLAccountID
        LEFT JOIN Planning.BudgetLineItem bli2
            ON bli2.BudgetHeaderID = @BudgetHeaderID
            AND bli2.GLAccountID = ip.PartnerAccountID;
        
        -- Update reconciliation status
        UPDATE ip
        SET ReconciliationStatus = 
            CASE 
                WHEN ip.IsWithinTolerance = 1 THEN 'RECONCILED'
                WHEN EXISTS (
                    SELECT 1 FROM #ReconciliationDetails rd 
                    WHERE rd.PairID = ip.PairID AND rd.MatchType = 'EXACT'
                ) THEN 'MATCHED'
                WHEN EXISTS (
                    SELECT 1 FROM #ReconciliationDetails rd 
                    WHERE rd.PairID = ip.PairID AND rd.MatchType = 'PARTIAL'
                ) THEN 'PARTIAL_MATCH'
                ELSE 'UNRECONCILED'
            END
        FROM #IntercompanyPairs ip;
        
        -- =====================================================================
        -- Auto-create adjustment entries if requested
        -- =====================================================================
        IF @AutoCreateAdjustments = 1
        BEGIN
            DECLARE @JournalID BIGINT;
            DECLARE @LineNum INT = 0;
            
            -- Create consolidation journal for adjustments
            INSERT INTO Planning.ConsolidationJournal (
                JournalNumber, JournalType, BudgetHeaderID, FiscalPeriodID,
                PostingDate, Description, StatusCode
            )
            SELECT 
                'ICR-' + FORMAT(@EffectiveDate, 'yyyyMMdd') + '-' + LEFT(CAST(@ReconciliationID AS VARCHAR(36)), 8),
                'ELIMINATION',
                @BudgetHeaderID,
                (SELECT TOP 1 FiscalPeriodID FROM Planning.FiscalPeriod 
                 WHERE @EffectiveDate BETWEEN PeriodStartDate AND PeriodEndDate),
                @EffectiveDate,
                'Auto-generated intercompany reconciliation adjustment',
                'DRAFT';
            
            SET @JournalID = SCOPE_IDENTITY();
            
            -- Insert adjustment lines for unreconciled pairs
            INSERT INTO Planning.ConsolidationJournalLine (
                JournalID, LineNumber, GLAccountID, CostCenterID,
                DebitAmount, CreditAmount, Description
            )
            SELECT 
                @JournalID,
                ROW_NUMBER() OVER (ORDER BY ip.PairID),
                ip.GLAccountID,
                (SELECT TOP 1 CostCenterID FROM Planning.CostCenter 
                 WHERE CostCenterCode LIKE ip.Entity1Code + '%'),
                CASE WHEN ip.Variance > 0 THEN ip.Variance ELSE 0 END,
                CASE WHEN ip.Variance < 0 THEN ABS(ip.Variance) ELSE 0 END,
                CONCAT('IC Adjustment: ', ip.Entity1Code, ' <-> ', ip.Entity2Code)
            FROM #IntercompanyPairs ip
            WHERE ip.ReconciliationStatus = 'UNRECONCILED'
              AND ABS(ip.Variance) > @ToleranceAmount;
        END
        
        -- =====================================================================
        -- Build XML report using FOR XML PATH with complex nesting
        -- =====================================================================
        SET @ReconciliationReportXML = (
            SELECT 
                @ReconciliationID AS '@ReconciliationID',
                @EffectiveDate AS '@ReconciliationDate',
                @BudgetHeaderID AS '@BudgetHeaderID',
                @ToleranceAmount AS '@ToleranceAmount',
                @TolerancePercent AS '@TolerancePercent',
                -- Summary statistics
                (
                    SELECT 
                        (SELECT COUNT(*) FROM #IntercompanyPairs) AS TotalPairs,
                        (SELECT COUNT(*) FROM #IntercompanyPairs WHERE ReconciliationStatus = 'RECONCILED') AS Reconciled,
                        (SELECT COUNT(*) FROM #IntercompanyPairs WHERE ReconciliationStatus = 'UNRECONCILED') AS Unreconciled,
                        (SELECT SUM(ABS(Variance)) FROM #IntercompanyPairs) AS TotalVariance,
                        (SELECT SUM(ABS(Variance)) FROM #IntercompanyPairs WHERE IsWithinTolerance = 0) AS OutOfToleranceVariance
                    FOR XML PATH('Statistics'), TYPE
                ),
                -- Entity summary
                (
                    SELECT 
                        EntityCode AS '@Code',
                        EntityName AS '@Name',
                        (
                            SELECT COUNT(*) FROM #IntercompanyPairs ip 
                            WHERE ip.Entity1Code = el.EntityCode OR ip.Entity2Code = el.EntityCode
                        ) AS PairCount,
                        (
                            SELECT SUM(ABS(ip.Variance)) FROM #IntercompanyPairs ip 
                            WHERE ip.Entity1Code = el.EntityCode OR ip.Entity2Code = el.EntityCode
                        ) AS TotalVariance
                    FROM @EntityList el
                    WHERE el.IncludeFlag = 1
                    FOR XML PATH('Entity'), ROOT('Entities'), TYPE
                ),
                -- Detailed pairs
                (
                    SELECT 
                        ip.PairID AS '@ID',
                        ip.Entity1Code AS '@Entity1',
                        ip.Entity2Code AS '@Entity2',
                        ip.ReconciliationStatus AS '@Status',
                        gla.AccountNumber AS Account,
                        ip.Entity1Amount AS Amount1,
                        ip.Entity2Amount AS Amount2,
                        ip.Variance AS Variance,
                        ip.VariancePercent AS VariancePercent,
                        ip.IsWithinTolerance AS WithinTolerance,
                        -- Nested detail lines
                        (
                            SELECT 
                                rd.MatchType AS '@Type',
                                rd.MatchScore AS '@Score',
                                rd.SourceLineItemID AS SourceID,
                                rd.TargetLineItemID AS TargetID,
                                rd.MatchDetails AS Details
                            FROM #ReconciliationDetails rd
                            WHERE rd.PairID = ip.PairID
                            FOR XML PATH('Match'), TYPE
                        ) AS Matches
                    FROM #IntercompanyPairs ip
                    INNER JOIN Planning.GLAccount gla ON ip.GLAccountID = gla.GLAccountID
                    ORDER BY 
                        CASE ip.ReconciliationStatus 
                            WHEN 'UNRECONCILED' THEN 1 
                            WHEN 'PARTIAL_MATCH' THEN 2 
                            ELSE 3 
                        END,
                        ABS(ip.Variance) DESC
                    FOR XML PATH('Pair'), ROOT('IntercompanyPairs'), TYPE
                )
            FOR XML PATH('ReconciliationReport')
        );
        
        -- Set output parameters
        SELECT 
            @UnreconciledCount = COUNT(*),
            @TotalVarianceAmount = SUM(ABS(Variance))
        FROM #IntercompanyPairs
        WHERE ReconciliationStatus = 'UNRECONCILED';
        
    END TRY
    BEGIN CATCH
        SET @ErrorMsg = ERROR_MESSAGE();
        
        -- Build error report as XML
        SET @ReconciliationReportXML = (
            SELECT 
                'ERROR' AS '@Status',
                ERROR_NUMBER() AS ErrorNumber,
                @ErrorMsg AS ErrorMessage,
                ERROR_LINE() AS ErrorLine,
                ERROR_PROCEDURE() AS ErrorProcedure
            FOR XML PATH('ReconciliationError')
        );
        
        THROW;
    END CATCH
    
    -- Cleanup
    DROP TABLE IF EXISTS #IntercompanyPairs;
    DROP TABLE IF EXISTS #ReconciliationDetails;
END
GO
