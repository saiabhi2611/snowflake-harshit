/*
    AllocationRule - Rules for cost allocation across cost centers
    Dependencies: CostCenter, GLAccount
*/
CREATE TABLE Planning.AllocationRule (
    AllocationRuleID        INT IDENTITY(1,1) NOT NULL,
    RuleCode                VARCHAR(30) NOT NULL,
    RuleName                NVARCHAR(100) NOT NULL,
    RuleDescription         NVARCHAR(500) NULL,
    RuleType                VARCHAR(20) NOT NULL,  -- DIRECT, STEP_DOWN, RECIPROCAL, ACTIVITY_BASED
    AllocationMethod        VARCHAR(20) NOT NULL,  -- FIXED_PCT, HEADCOUNT, SQUARE_FOOTAGE, REVENUE, CUSTOM
    -- Source specification
    SourceCostCenterID      INT NULL,  -- NULL means all cost centers matching pattern
    SourceCostCenterPattern VARCHAR(50) NULL,  -- Regex pattern for cost center matching
    SourceAccountPattern    VARCHAR(50) NULL,  -- Regex pattern for account matching
    -- Target specification using XML for flexibility
    TargetSpecification     XML NOT NULL,  -- Complex target definitions
    -- Calculation parameters
    AllocationBasis         VARCHAR(30) NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,
    RoundingMethod          VARCHAR(10) NOT NULL DEFAULT 'NEAREST',  -- NEAREST, UP, DOWN, NONE
    RoundingPrecision       TINYINT NOT NULL DEFAULT 2,
    MinimumAmount           DECIMAL(19,4) NULL,
    -- Execution order for step-down allocations
    ExecutionSequence       INT NOT NULL DEFAULT 100,
    DependsOnRuleID         INT NULL,
    -- Validity
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    IsActive                BIT NOT NULL DEFAULT 1,
    -- Audit
    CreatedByUserID         INT NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedByUserID        INT NULL,
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_AllocationRule PRIMARY KEY CLUSTERED (AllocationRuleID),
    CONSTRAINT UQ_AllocationRule_Code UNIQUE (RuleCode),
    CONSTRAINT FK_AllocationRule_SourceCC FOREIGN KEY (SourceCostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_AllocationRule_DependsOn FOREIGN KEY (DependsOnRuleID) 
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_AllocationRule_Type CHECK (RuleType IN ('DIRECT','STEP_DOWN','RECIPROCAL','ACTIVITY_BASED')),
    CONSTRAINT CK_AllocationRule_Rounding CHECK (RoundingMethod IN ('NEAREST','UP','DOWN','NONE'))
);
GO

-- Primary XML index on target specification
CREATE PRIMARY XML INDEX PXML_AllocationRule_TargetSpec
ON Planning.AllocationRule (TargetSpecification);
GO
