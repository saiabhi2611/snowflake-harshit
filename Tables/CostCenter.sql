/*
    CostCenter - Organizational hierarchy for cost allocation
    Dependencies: None (base table)
    
    Note: Uses HierarchyID which has no Snowflake equivalent
*/
CREATE TABLE Planning.CostCenter (
    CostCenterID            INT IDENTITY(1,1) NOT NULL,
    CostCenterCode          VARCHAR(20) NOT NULL,
    CostCenterName          NVARCHAR(100) NOT NULL,
    ParentCostCenterID      INT NULL,
    HierarchyPath           HIERARCHYID NULL,  -- No Snowflake equivalent
    HierarchyLevel          AS HierarchyPath.GetLevel() PERSISTED,  -- Computed column from HierarchyID
    ManagerEmployeeID       INT NULL,
    DepartmentCode          VARCHAR(10) NULL,
    IsActive                BIT NOT NULL DEFAULT 1,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    AllocationWeight        DECIMAL(5,4) NOT NULL DEFAULT 1.0000,
    ValidFrom               DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,  -- Temporal table
    ValidTo                 DATETIME2(7) GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo),  -- System-versioned temporal table
    CONSTRAINT PK_CostCenter PRIMARY KEY CLUSTERED (CostCenterID),
    CONSTRAINT UQ_CostCenter_Code UNIQUE (CostCenterCode),
    CONSTRAINT FK_CostCenter_Parent FOREIGN KEY (ParentCostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT CK_CostCenter_Weight CHECK (AllocationWeight BETWEEN 0 AND 1)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = Planning.CostCenterHistory));
GO

-- Spatial index on HierarchyID - no equivalent in Snowflake
CREATE UNIQUE INDEX IX_CostCenter_Hierarchy 
ON Planning.CostCenter (HierarchyPath);
GO
