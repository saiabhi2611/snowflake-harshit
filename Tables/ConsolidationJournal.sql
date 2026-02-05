/*
    ConsolidationJournal - Journal entries for consolidation adjustments
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod
*/
CREATE TABLE Planning.ConsolidationJournal (
    JournalID               BIGINT IDENTITY(1,1) NOT NULL,
    JournalNumber           VARCHAR(30) NOT NULL,
    JournalType             VARCHAR(20) NOT NULL,  -- ELIMINATION, RECLASSIFICATION, TRANSLATION, ADJUSTMENT
    BudgetHeaderID          INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    PostingDate             DATE NOT NULL,
    Description             NVARCHAR(500) NULL,
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    -- Entity tracking for multi-entity consolidation
    SourceEntityCode        VARCHAR(20) NULL,
    TargetEntityCode        VARCHAR(20) NULL,
    -- Reversal handling
    IsAutoReverse           BIT NOT NULL DEFAULT 0,
    ReversalPeriodID        INT NULL,
    ReversedFromJournalID   BIGINT NULL,
    IsReversed              BIT NOT NULL DEFAULT 0,
    -- Totals (denormalized for performance)
    TotalDebits             DECIMAL(19,4) NOT NULL DEFAULT 0,
    TotalCredits            DECIMAL(19,4) NOT NULL DEFAULT 0,
    IsBalanced              AS CASE WHEN TotalDebits = TotalCredits THEN 1 ELSE 0 END,
    -- Approval workflow
    PreparedByUserID        INT NULL,
    PreparedDateTime        DATETIME2(7) NULL,
    ReviewedByUserID        INT NULL,
    ReviewedDateTime        DATETIME2(7) NULL,
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        DATETIME2(7) NULL,
    PostedByUserID          INT NULL,
    PostedDateTime          DATETIME2(7) NULL,
    -- Attachments stored as FILESTREAM (no Snowflake equivalent)
    AttachmentData          VARBINARY(MAX) FILESTREAM NULL,
    AttachmentRowGuid       UNIQUEIDENTIFIER ROWGUIDCOL NOT NULL DEFAULT NEWSEQUENTIALID(),
    CONSTRAINT PK_ConsolidationJournal PRIMARY KEY CLUSTERED (JournalID),
    CONSTRAINT UQ_ConsolidationJournal_Number UNIQUE (JournalNumber),
    CONSTRAINT FK_ConsolidationJournal_Header FOREIGN KEY (BudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_ConsolidationJournal_Period FOREIGN KEY (FiscalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversalPeriod FOREIGN KEY (ReversalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversedFrom FOREIGN KEY (ReversedFromJournalID) 
        REFERENCES Planning.ConsolidationJournal (JournalID)
);
GO

-- Unique constraint with ROWGUIDCOL for FILESTREAM
CREATE UNIQUE NONCLUSTERED INDEX IX_ConsolidationJournal_RowGuid
ON Planning.ConsolidationJournal (AttachmentRowGuid);
GO
