/*
    AllocationResultTableType - Table type for returning allocation results
    Dependencies: None
    
    NOTE: No Snowflake equivalent - must be refactored to temp tables or VARIANT
*/
CREATE TYPE Planning.AllocationResultTableType AS TABLE (
    SourceBudgetLineItemID  BIGINT NOT NULL,
    TargetCostCenterID      INT NOT NULL,
    TargetGLAccountID       INT NOT NULL,
    AllocatedAmount         DECIMAL(19,4) NOT NULL,
    AllocationPercentage    DECIMAL(8,6) NOT NULL,
    AllocationRuleID        INT NOT NULL,
    ProcessingSequence      INT NOT NULL,
    INDEX IX_Source (SourceBudgetLineItemID),
    INDEX IX_Target (TargetCostCenterID, TargetGLAccountID)
);
GO
