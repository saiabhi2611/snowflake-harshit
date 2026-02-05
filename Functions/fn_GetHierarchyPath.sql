/*
    fn_GetHierarchyPath - Builds the full hierarchy path string for a cost center
    Dependencies: CostCenter
    
    Challenges for Snowflake:
    - Recursive scalar function calls
    - String concatenation patterns differ
    - HierarchyID type not available
*/
CREATE FUNCTION Planning.fn_GetHierarchyPath (
    @CostCenterID INT,
    @Delimiter NVARCHAR(5) = N' > '
)
RETURNS NVARCHAR(1000)
AS
BEGIN
    DECLARE @Path NVARCHAR(1000) = N'';
    DECLARE @CurrentID INT = @CostCenterID;
    DECLARE @ParentID INT;
    DECLARE @Name NVARCHAR(100);
    DECLARE @Depth INT = 0;
    DECLARE @MaxDepth INT = 20;  -- Prevent infinite loops
    
    -- Traverse up the hierarchy
    WHILE @CurrentID IS NOT NULL AND @Depth < @MaxDepth
    BEGIN
        SELECT @Name = CostCenterName,
               @ParentID = ParentCostCenterID
        FROM Planning.CostCenter
        WHERE CostCenterID = @CurrentID;
        
        IF @Path = N''
            SET @Path = @Name;
        ELSE
            SET @Path = @Name + @Delimiter + @Path;
        
        SET @CurrentID = @ParentID;
        SET @Depth = @Depth + 1;
    END
    
    RETURN @Path;
END
GO
