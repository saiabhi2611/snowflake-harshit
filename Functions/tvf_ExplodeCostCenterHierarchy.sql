/*
    tvf_ExplodeCostCenterHierarchy - Multi-statement TVF to explode hierarchy
    Dependencies: CostCenter
    
    Challenges for Snowflake:
    - Multi-statement TVFs don't exist in Snowflake
    - Would need to be refactored to recursive CTE or JavaScript UDF
    - Table variable usage inside function
    - WHILE loop logic
*/
CREATE FUNCTION Planning.tvf_ExplodeCostCenterHierarchy (
    @RootCostCenterID   INT = NULL,
    @MaxDepth           INT = 10,
    @IncludeInactive    BIT = 0,
    @AsOfDate           DATE = NULL
)
RETURNS @HierarchyTable TABLE (
    CostCenterID        INT NOT NULL,
    CostCenterCode      VARCHAR(20) NOT NULL,
    CostCenterName      NVARCHAR(100) NOT NULL,
    ParentCostCenterID  INT NULL,
    HierarchyLevel      INT NOT NULL,
    HierarchyPath       NVARCHAR(500) NOT NULL,
    SortPath            VARCHAR(500) NOT NULL,
    IsLeaf              BIT NOT NULL,
    ChildCount          INT NOT NULL,
    CumulativeWeight    DECIMAL(18,10) NOT NULL,
    PRIMARY KEY CLUSTERED (CostCenterID)
)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @CurrentLevel INT = 0;
    DECLARE @RowsInserted INT = 1;
    DECLARE @EffectiveDate DATE = ISNULL(@AsOfDate, GETDATE());
    
    -- Insert root level
    INSERT INTO @HierarchyTable (
        CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
        HierarchyLevel, HierarchyPath, SortPath, IsLeaf, ChildCount, CumulativeWeight
    )
    SELECT 
        cc.CostCenterID,
        cc.CostCenterCode,
        cc.CostCenterName,
        cc.ParentCostCenterID,
        0 AS HierarchyLevel,
        CAST(cc.CostCenterName AS NVARCHAR(500)) AS HierarchyPath,
        CAST(RIGHT('0000000000' + CAST(cc.CostCenterID AS VARCHAR(10)), 10) AS VARCHAR(500)) AS SortPath,
        0 AS IsLeaf,
        0 AS ChildCount,
        cc.AllocationWeight
    FROM Planning.CostCenter cc
    WHERE (@RootCostCenterID IS NULL AND cc.ParentCostCenterID IS NULL)
       OR cc.CostCenterID = @RootCostCenterID
      AND (cc.IsActive = 1 OR @IncludeInactive = 1)
      AND cc.EffectiveFromDate <= @EffectiveDate
      AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= @EffectiveDate);
    
    -- Iteratively add children
    WHILE @RowsInserted > 0 AND @CurrentLevel < @MaxDepth
    BEGIN
        SET @CurrentLevel = @CurrentLevel + 1;
        
        INSERT INTO @HierarchyTable (
            CostCenterID, CostCenterCode, CostCenterName, ParentCostCenterID,
            HierarchyLevel, HierarchyPath, SortPath, IsLeaf, ChildCount, CumulativeWeight
        )
        SELECT 
            cc.CostCenterID,
            cc.CostCenterCode,
            cc.CostCenterName,
            cc.ParentCostCenterID,
            @CurrentLevel,
            h.HierarchyPath + N' > ' + cc.CostCenterName,
            h.SortPath + '/' + RIGHT('0000000000' + CAST(cc.CostCenterID AS VARCHAR(10)), 10),
            0 AS IsLeaf,
            0 AS ChildCount,
            h.CumulativeWeight * cc.AllocationWeight
        FROM Planning.CostCenter cc
        INNER JOIN @HierarchyTable h ON cc.ParentCostCenterID = h.CostCenterID
        WHERE h.HierarchyLevel = @CurrentLevel - 1
          AND (cc.IsActive = 1 OR @IncludeInactive = 1)
          AND cc.EffectiveFromDate <= @EffectiveDate
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= @EffectiveDate)
          AND NOT EXISTS (SELECT 1 FROM @HierarchyTable x WHERE x.CostCenterID = cc.CostCenterID);
        
        SET @RowsInserted = @@ROWCOUNT;
    END
    
    -- Update leaf flags and child counts
    UPDATE h
    SET IsLeaf = CASE 
            WHEN EXISTS (
                SELECT 1 FROM Planning.CostCenter cc 
                WHERE cc.ParentCostCenterID = h.CostCenterID
                  AND (cc.IsActive = 1 OR @IncludeInactive = 1)
            ) THEN 0 
            ELSE 1 
        END,
        ChildCount = (
            SELECT COUNT(*) 
            FROM @HierarchyTable c 
            WHERE c.ParentCostCenterID = h.CostCenterID
        )
    FROM @HierarchyTable h;
    
    RETURN;
END
GO
