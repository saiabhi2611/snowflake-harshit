/*
    usp_GenerateRollingForecast - Generates rolling forecast with statistical projections
    
    Dependencies:
        - Tables: BudgetHeader, BudgetLineItem, FiscalPeriod, CostCenter, GLAccount
        - Functions: fn_GetAllocationFactor, tvf_GetBudgetVariance
        - Views: vw_BudgetConsolidationSummary
    
    ============================================================================
    SNOWFLAKE MIGRATION CHALLENGES:
    ============================================================================
    1. PIVOT with dynamic columns - Different syntax in Snowflake
    2. Window functions with ROWS BETWEEN and complex frames
    3. LAG/LEAD with multiple offsets computed dynamically
    4. PERCENTILE_CONT and statistical aggregates
    5. FOR XML PATH string concatenation pattern - Use LISTAGG in Snowflake
    6. Dynamic PIVOT generation with sp_executesql
    7. Global temp tables (##) - Not available in Snowflake
    8. OPENJSON for JSON parsing - Different in Snowflake
    9. Recursive forecast calculation with running totals
    10. COMPUTE BY clause (deprecated but still used)
    11. Complex CASE expressions with subqueries
    12. APPLY with derived tables
    ============================================================================
*/
CREATE PROCEDURE Planning.usp_GenerateRollingForecast
    @BaseBudgetHeaderID         INT,
    @HistoricalPeriods          INT = 12,           -- Months of history to analyze
    @ForecastPeriods            INT = 12,           -- Months to forecast
    @ForecastMethod             VARCHAR(30) = 'WEIGHTED_AVERAGE',  -- WEIGHTED_AVERAGE, LINEAR_TREND, EXPONENTIAL, SEASONAL
    @SeasonalityJSON            NVARCHAR(MAX) = NULL,  -- JSON array of seasonal factors
    @GrowthRateOverride         DECIMAL(8,4) = NULL,
    @ConfidenceLevel            DECIMAL(5,4) = 0.95,
    @OutputFormat               VARCHAR(20) = 'DETAIL',  -- DETAIL, SUMMARY, PIVOT
    @TargetBudgetHeaderID       INT = NULL OUTPUT,
    @ForecastAccuracyMetrics    NVARCHAR(MAX) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = SYSUTCDATETIME();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @SourceFiscalYear SMALLINT;
    DECLARE @SourceStartPeriod INT;
    DECLARE @BaselineEndPeriodID INT;
    
    -- =========================================================================
    -- Create global temp table for cross-session visibility (debugging)
    -- =========================================================================
    IF OBJECT_ID('tempdb..##ForecastWorkspace') IS NOT NULL
        DROP TABLE ##ForecastWorkspace;
    
    CREATE TABLE ##ForecastWorkspace (
        WorkspaceID             INT IDENTITY(1,1) PRIMARY KEY,
        SessionID               INT DEFAULT @@SPID,
        GLAccountID             INT,
        CostCenterID            INT,
        FiscalPeriodID          INT,
        PeriodSequence          INT,  -- Relative position
        ActualAmount            DECIMAL(19,4),
        ForecastAmount          DECIMAL(19,4),
        LowerBound              DECIMAL(19,4),  -- Confidence interval
        UpperBound              DECIMAL(19,4),
        SeasonalFactor          DECIMAL(8,6),
        TrendComponent          DECIMAL(19,4),
        CyclicalComponent       DECIMAL(19,4),
        Residual                DECIMAL(19,4),
        WeightFactor            DECIMAL(8,6),
        IsForecast              BIT DEFAULT 0,
        CalculationStep         INT
    );
    
    -- =========================================================================
    -- Parse seasonality JSON using OPENJSON
    -- =========================================================================
    DECLARE @SeasonalFactors TABLE (
        MonthNumber     INT PRIMARY KEY,
        SeasonalFactor  DECIMAL(8,6)
    );
    
    IF @SeasonalityJSON IS NOT NULL
    BEGIN
        INSERT INTO @SeasonalFactors (MonthNumber, SeasonalFactor)
        SELECT 
            CAST([key] AS INT) + 1,  -- Convert 0-based to 1-based month
            CAST([value] AS DECIMAL(8,6))
        FROM OPENJSON(@SeasonalityJSON);  -- OPENJSON not in Snowflake, use PARSE_JSON
    END
    ELSE
    BEGIN
        -- Default: no seasonality
        INSERT INTO @SeasonalFactors (MonthNumber, SeasonalFactor)
        SELECT n, 1.0
        FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) AS T(n);
    END
    
    BEGIN TRY
        -- Get base budget info
        SELECT 
            @SourceFiscalYear = FiscalYear,
            @SourceStartPeriod = StartPeriodID
        FROM Planning.BudgetHeader
        WHERE BudgetHeaderID = @BaseBudgetHeaderID;
        
        IF @SourceFiscalYear IS NULL
        BEGIN
            SET @ErrorMessage = 'Base budget header not found';
            THROW 50100, @ErrorMessage, 1;
        END
        
        -- =====================================================================
        -- Populate historical data with window calculations
        -- =====================================================================
        INSERT INTO ##ForecastWorkspace (
            GLAccountID, CostCenterID, FiscalPeriodID, PeriodSequence,
            ActualAmount, SeasonalFactor, WeightFactor, IsForecast
        )
        SELECT 
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FiscalPeriodID,
            ROW_NUMBER() OVER (
                PARTITION BY bli.GLAccountID, bli.CostCenterID 
                ORDER BY fp.FiscalYear, fp.FiscalMonth
            ) AS PeriodSequence,
            bli.FinalAmount,
            ISNULL(sf.SeasonalFactor, 1.0),
            -- Exponential decay weight: more recent = higher weight
            POWER(0.9, @HistoricalPeriods - ROW_NUMBER() OVER (
                PARTITION BY bli.GLAccountID, bli.CostCenterID 
                ORDER BY fp.FiscalYear, fp.FiscalMonth
            )) AS WeightFactor,
            0 AS IsForecast
        FROM Planning.BudgetLineItem bli
        INNER JOIN Planning.FiscalPeriod fp ON bli.FiscalPeriodID = fp.FiscalPeriodID
        LEFT JOIN @SeasonalFactors sf ON fp.FiscalMonth = sf.MonthNumber
        WHERE bli.BudgetHeaderID = @BaseBudgetHeaderID
          AND fp.FiscalYear >= @SourceFiscalYear - 1;  -- Include prior year for trend
        
        -- =====================================================================
        -- Calculate trend components using window functions
        -- =====================================================================
        ;WITH TrendCalc AS (
            SELECT 
                WorkspaceID,
                GLAccountID,
                CostCenterID,
                PeriodSequence,
                ActualAmount,
                -- Moving average with different windows
                AVG(ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS MA3,
                AVG(ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence 
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ) AS MA6,
                AVG(ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) AS MA12,
                -- Trend calculation using linear regression components
                -- Snowflake has different syntax for these aggregations
                COUNT(*) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS N,
                SUM(PeriodSequence) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS SumX,
                SUM(ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS SumY,
                SUM(PeriodSequence * ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS SumXY,
                SUM(PeriodSequence * PeriodSequence) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS SumXX,
                -- LAG/LEAD for comparison
                LAG(ActualAmount, 1, 0) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence
                ) AS PrevAmount1,
                LAG(ActualAmount, 12, NULL) OVER (
                    PARTITION BY GLAccountID, CostCenterID 
                    ORDER BY PeriodSequence
                ) AS SameMonthLastYear,
                -- Percentile calculations
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS MedianAmount,
                PERCENTILE_CONT(@ConfidenceLevel) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS UpperPercentile,
                PERCENTILE_CONT(1 - @ConfidenceLevel) WITHIN GROUP (ORDER BY ActualAmount) OVER (
                    PARTITION BY GLAccountID, CostCenterID
                ) AS LowerPercentile
            FROM ##ForecastWorkspace
            WHERE IsForecast = 0
        )
        UPDATE fw
        SET 
            TrendComponent = CASE 
                WHEN tc.N > 1 AND (tc.N * tc.SumXX - tc.SumX * tc.SumX) <> 0 
                THEN (tc.N * tc.SumXY - tc.SumX * tc.SumY) / (tc.N * tc.SumXX - tc.SumX * tc.SumX)
                ELSE 0 
            END,
            CyclicalComponent = CASE 
                WHEN tc.SameMonthLastYear IS NOT NULL AND tc.SameMonthLastYear <> 0
                THEN (tc.ActualAmount - tc.SameMonthLastYear) / tc.SameMonthLastYear
                ELSE 0
            END,
            Residual = tc.ActualAmount - tc.MA12,
            LowerBound = tc.LowerPercentile,
            UpperBound = tc.UpperPercentile
        FROM ##ForecastWorkspace fw
        INNER JOIN TrendCalc tc ON fw.WorkspaceID = tc.WorkspaceID;
        
        -- =====================================================================
        -- Generate forecast periods and calculate forecasts
        -- =====================================================================
        -- Get last actual period
        SELECT @BaselineEndPeriodID = MAX(FiscalPeriodID)
        FROM ##ForecastWorkspace
        WHERE IsForecast = 0;
        
        -- Generate forecast period skeleton
        ;WITH FuturePeriods AS (
            SELECT 
                fp.FiscalPeriodID,
                fp.FiscalMonth,
                ROW_NUMBER() OVER (ORDER BY fp.FiscalYear, fp.FiscalMonth) AS FutureSequence
            FROM Planning.FiscalPeriod fp
            WHERE fp.FiscalPeriodID > @BaselineEndPeriodID
              AND fp.IsClosed = 0
        ),
        BaselineStats AS (
            SELECT 
                GLAccountID,
                CostCenterID,
                MAX(PeriodSequence) AS LastActualSequence,
                AVG(ActualAmount * WeightFactor) / NULLIF(AVG(WeightFactor), 0) AS WeightedAvg,
                AVG(TrendComponent) AS AvgTrend,
                STDEV(ActualAmount) AS StdDev
            FROM ##ForecastWorkspace
            WHERE IsForecast = 0
            GROUP BY GLAccountID, CostCenterID
        )
        INSERT INTO ##ForecastWorkspace (
            GLAccountID, CostCenterID, FiscalPeriodID, PeriodSequence,
            ForecastAmount, SeasonalFactor, LowerBound, UpperBound, IsForecast, CalculationStep
        )
        SELECT 
            bs.GLAccountID,
            bs.CostCenterID,
            fp.FiscalPeriodID,
            bs.LastActualSequence + fp.FutureSequence,
            -- Different forecast methods
            CASE @ForecastMethod
                WHEN 'WEIGHTED_AVERAGE' THEN
                    bs.WeightedAvg * ISNULL(sf.SeasonalFactor, 1.0) * 
                    POWER(1 + ISNULL(@GrowthRateOverride, bs.AvgTrend), fp.FutureSequence)
                WHEN 'LINEAR_TREND' THEN
                    bs.WeightedAvg + (bs.AvgTrend * (bs.LastActualSequence + fp.FutureSequence))
                WHEN 'EXPONENTIAL' THEN
                    bs.WeightedAvg * POWER(1 + ISNULL(@GrowthRateOverride, 0.02), fp.FutureSequence)
                WHEN 'SEASONAL' THEN
                    bs.WeightedAvg * sf.SeasonalFactor * 
                    (1 + ISNULL(@GrowthRateOverride, 0) * fp.FutureSequence / 12)
                ELSE
                    bs.WeightedAvg
            END,
            ISNULL(sf.SeasonalFactor, 1.0),
            -- Confidence interval bounds
            CASE @ForecastMethod
                WHEN 'WEIGHTED_AVERAGE' THEN
                    bs.WeightedAvg - (1.96 * bs.StdDev * SQRT(fp.FutureSequence))
                ELSE
                    bs.WeightedAvg * 0.8
            END,
            CASE @ForecastMethod
                WHEN 'WEIGHTED_AVERAGE' THEN
                    bs.WeightedAvg + (1.96 * bs.StdDev * SQRT(fp.FutureSequence))
                ELSE
                    bs.WeightedAvg * 1.2
            END,
            1 AS IsForecast,
            fp.FutureSequence
        FROM BaselineStats bs
        CROSS JOIN FuturePeriods fp
        LEFT JOIN @SeasonalFactors sf ON 
            (SELECT FiscalMonth FROM Planning.FiscalPeriod WHERE FiscalPeriodID = fp.FiscalPeriodID) = sf.MonthNumber
        WHERE fp.FutureSequence <= @ForecastPeriods;
        
        -- =====================================================================
        -- Create target budget header and insert forecast data
        -- =====================================================================
        INSERT INTO Planning.BudgetHeader (
            BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear,
            StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode, Notes
        )
        SELECT 
            BudgetCode + '_FORECAST_' + FORMAT(GETDATE(), 'yyyyMMdd'),
            BudgetName + ' - Rolling Forecast',
            'ROLLING',
            'FORECAST',
            FiscalYear,
            @SourceStartPeriod,
            (SELECT MAX(FiscalPeriodID) FROM ##ForecastWorkspace WHERE IsForecast = 1),
            @BaseBudgetHeaderID,
            'DRAFT',
            CONCAT('Generated by usp_GenerateRollingForecast at ', @StartTime, 
                   ' using method: ', @ForecastMethod)
        FROM Planning.BudgetHeader
        WHERE BudgetHeaderID = @BaseBudgetHeaderID;
        
        SET @TargetBudgetHeaderID = SCOPE_IDENTITY();
        
        -- Insert forecast line items
        INSERT INTO Planning.BudgetLineItem (
            BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem
        )
        SELECT 
            @TargetBudgetHeaderID,
            GLAccountID,
            CostCenterID,
            FiscalPeriodID,
            ForecastAmount,
            0,
            @ForecastMethod,
            'ROLLING_FORECAST'
        FROM ##ForecastWorkspace
        WHERE IsForecast = 1
          AND ForecastAmount IS NOT NULL;
        
        -- =====================================================================
        -- Generate output based on format
        -- =====================================================================
        IF @OutputFormat = 'PIVOT'
        BEGIN
            -- Dynamic PIVOT - Very different in Snowflake
            DECLARE @PivotColumns NVARCHAR(MAX);
            DECLARE @DynamicSQL NVARCHAR(MAX);
            
            -- Build column list using FOR XML PATH
            SELECT @PivotColumns = STUFF((
                SELECT DISTINCT ',' + QUOTENAME(fp.PeriodName)
                FROM ##ForecastWorkspace fw
                INNER JOIN Planning.FiscalPeriod fp ON fw.FiscalPeriodID = fp.FiscalPeriodID
                WHERE fw.IsForecast = 1
                ORDER BY ',' + QUOTENAME(fp.PeriodName)
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 1, '');
            
            SET @DynamicSQL = N'
                SELECT 
                    gla.AccountNumber,
                    gla.AccountName,
                    cc.CostCenterCode,
                    cc.CostCenterName,
                    ' + @PivotColumns + N'
                FROM (
                    SELECT 
                        fw.GLAccountID,
                        fw.CostCenterID,
                        fp.PeriodName,
                        fw.ForecastAmount
                    FROM ##ForecastWorkspace fw
                    INNER JOIN Planning.FiscalPeriod fp ON fw.FiscalPeriodID = fp.FiscalPeriodID
                    WHERE fw.IsForecast = 1
                ) AS SourceData
                PIVOT (
                    SUM(ForecastAmount)
                    FOR PeriodName IN (' + @PivotColumns + N')
                ) AS PivotTable
                INNER JOIN Planning.GLAccount gla ON PivotTable.GLAccountID = gla.GLAccountID
                INNER JOIN Planning.CostCenter cc ON PivotTable.CostCenterID = cc.CostCenterID
                ORDER BY gla.AccountNumber, cc.CostCenterCode;
            ';
            
            EXEC sp_executesql @DynamicSQL;
        END
        ELSE IF @OutputFormat = 'SUMMARY'
        BEGIN
            SELECT 
                gla.AccountType,
                fp.FiscalYear,
                fp.FiscalQuarter,
                SUM(fw.ForecastAmount) AS TotalForecast,
                SUM(fw.LowerBound) AS TotalLowerBound,
                SUM(fw.UpperBound) AS TotalUpperBound
            FROM ##ForecastWorkspace fw
            INNER JOIN Planning.GLAccount gla ON fw.GLAccountID = gla.GLAccountID
            INNER JOIN Planning.FiscalPeriod fp ON fw.FiscalPeriodID = fp.FiscalPeriodID
            WHERE fw.IsForecast = 1
            GROUP BY gla.AccountType, fp.FiscalYear, fp.FiscalQuarter
            ORDER BY gla.AccountType, fp.FiscalYear, fp.FiscalQuarter;
        END
        
        -- =====================================================================
        -- Calculate and return accuracy metrics as JSON
        -- =====================================================================
        SET @ForecastAccuracyMetrics = (
            SELECT 
                @ForecastMethod AS ForecastMethod,
                @HistoricalPeriods AS HistoricalPeriods,
                @ForecastPeriods AS ForecastPeriods,
                (SELECT COUNT(*) FROM ##ForecastWorkspace WHERE IsForecast = 0) AS ActualDataPoints,
                (SELECT COUNT(*) FROM ##ForecastWorkspace WHERE IsForecast = 1) AS ForecastDataPoints,
                (SELECT AVG(ABS(Residual)) FROM ##ForecastWorkspace WHERE IsForecast = 0) AS MeanAbsoluteResidual,
                (SELECT STDEV(ActualAmount) FROM ##ForecastWorkspace WHERE IsForecast = 0) AS HistoricalStdDev,
                DATEDIFF(MILLISECOND, @StartTime, SYSUTCDATETIME()) AS ExecutionTimeMs
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );
        
    END TRY
    BEGIN CATCH
        SET @ForecastAccuracyMetrics = (
            SELECT 
                'ERROR' AS Status,
                ERROR_NUMBER() AS ErrorNumber,
                ERROR_MESSAGE() AS ErrorMessage,
                ERROR_LINE() AS ErrorLine
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );
        
        THROW;
    END CATCH
    
    -- Cleanup
    DROP TABLE IF EXISTS ##ForecastWorkspace;
END
GO
