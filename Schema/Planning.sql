/*
    Planning Schema - Creates the schema for all financial planning objects
    
    This must be executed first before any other objects.
*/

-- Create schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Planning')
BEGIN
    EXEC('CREATE SCHEMA Planning');
END
GO

-- Grant permissions (example)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::Planning TO [PlanningUsers];
-- GRANT EXECUTE ON SCHEMA::Planning TO [PlanningUsers];
-- GRANT VIEW DEFINITION ON SCHEMA::Planning TO [PlanningUsers];
GO
