/*
    vw_AllocationRuleTargets - Parses XML target specifications from allocation rules
    Dependencies: AllocationRule, CostCenter
    
    Challenges for Snowflake:
    - XML parsing with .nodes() and .value() methods
    - XQuery expressions
    - CROSS APPLY with XML
*/
CREATE VIEW Planning.vw_AllocationRuleTargets
AS
SELECT 
    ar.AllocationRuleID,
    ar.RuleCode,
    ar.RuleName,
    ar.RuleType,
    ar.AllocationMethod,
    ar.ExecutionSequence,
    -- Parse XML target specifications
    TargetSpec.value('(@CostCenterID)[1]', 'INT') AS TargetCostCenterID,
    TargetSpec.value('(@CostCenterCode)[1]', 'VARCHAR(20)') AS TargetCostCenterCode,
    TargetSpec.value('(@AllocationPercentage)[1]', 'DECIMAL(8,6)') AS TargetAllocationPct,
    TargetSpec.value('(@Priority)[1]', 'INT') AS TargetPriority,
    TargetSpec.value('(AccountFilter/text())[1]', 'VARCHAR(50)') AS AccountFilter,
    TargetSpec.value('(ExcludePattern/text())[1]', 'VARCHAR(50)') AS ExcludePattern,
    -- Check for conditional allocations
    TargetSpec.exist('Conditions/Condition') AS HasConditions,
    TargetSpec.query('Conditions') AS ConditionsXml,
    -- Join to cost center for target details
    cc.CostCenterName AS TargetCostCenterName,
    cc.ParentCostCenterID AS TargetParentCostCenterID,
    cc.IsActive AS TargetIsActive,
    ar.EffectiveFromDate,
    ar.EffectiveToDate,
    ar.IsActive AS RuleIsActive
FROM Planning.AllocationRule ar
CROSS APPLY ar.TargetSpecification.nodes('/AllocationTargets/Target') AS T(TargetSpec)
LEFT JOIN Planning.CostCenter cc 
    ON cc.CostCenterID = TargetSpec.value('(@CostCenterID)[1]', 'INT')
       OR cc.CostCenterCode = TargetSpec.value('(@CostCenterCode)[1]', 'VARCHAR(20)');
GO
