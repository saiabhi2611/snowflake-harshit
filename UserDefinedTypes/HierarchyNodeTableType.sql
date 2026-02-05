/*
    HierarchyNodeTableType - For passing hierarchy traversal data
    Dependencies: None
*/
CREATE TYPE Planning.HierarchyNodeTableType AS TABLE (
    NodeID                  INT NOT NULL,
    ParentNodeID            INT NULL,
    NodeLevel               INT NOT NULL,
    NodePath                VARCHAR(500) NOT NULL,
    SortOrder               INT NOT NULL,
    IsLeaf                  BIT NOT NULL,
    AggregationWeight       DECIMAL(8,6) NOT NULL DEFAULT 1.0,
    PRIMARY KEY CLUSTERED (NodeID)
);
GO
