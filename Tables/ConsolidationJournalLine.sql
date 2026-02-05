/*
    ConsolidationJournalLine - Line items for consolidation journal entries
    Dependencies: ConsolidationJournal, GLAccount, CostCenter
*/
CREATE TABLE Planning.ConsolidationJournalLine (
    JournalLineID           BIGINT IDENTITY(1,1) NOT NULL,
    JournalID               BIGINT NOT NULL,
    LineNumber              INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    DebitAmount             DECIMAL(19,4) NOT NULL DEFAULT 0,
    CreditAmount            DECIMAL(19,4) NOT NULL DEFAULT 0,
    NetAmount               AS (DebitAmount - CreditAmount) PERSISTED,
    LocalCurrencyCode       CHAR(3) NOT NULL DEFAULT 'USD',
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ExchangeRate            DECIMAL(18,10) NULL,
    Description             NVARCHAR(255) NULL,
    ReferenceNumber         VARCHAR(50) NULL,
    -- Intercompany tracking
    PartnerEntityCode       VARCHAR(20) NULL,
    PartnerAccountID        INT NULL,
    -- Statistical tracking
    StatisticalQuantity     DECIMAL(18,6) NULL,
    StatisticalUOM          VARCHAR(10) NULL,
    -- Allocation tracking
    AllocationRuleID        INT NULL,
    -- Audit
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_ConsolidationJournalLine PRIMARY KEY CLUSTERED (JournalLineID),
    CONSTRAINT UQ_ConsolidationJournalLine_JournalLine UNIQUE (JournalID, LineNumber),
    CONSTRAINT FK_ConsolidationJournalLine_Journal FOREIGN KEY (JournalID) 
        REFERENCES Planning.ConsolidationJournal (JournalID) ON DELETE CASCADE,
    CONSTRAINT FK_ConsolidationJournalLine_Account FOREIGN KEY (GLAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_ConsolidationJournalLine_CostCenter FOREIGN KEY (CostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_ConsolidationJournalLine_AllocationRule FOREIGN KEY (AllocationRuleID) 
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_ConsolidationJournalLine_DebitCredit CHECK (
        (DebitAmount >= 0 AND CreditAmount >= 0) AND
        NOT (DebitAmount > 0 AND CreditAmount > 0)  -- Cannot have both
    )
);
GO

-- Columnstore for reporting
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_ConsolidationJournalLine
ON Planning.ConsolidationJournalLine (
    JournalID, GLAccountID, CostCenterID, 
    DebitAmount, CreditAmount, LocalCurrencyAmount
);
GO
