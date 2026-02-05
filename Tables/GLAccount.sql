/*
    GLAccount - General Ledger Account master
    Dependencies: None (base table)
*/
CREATE TABLE Planning.GLAccount (
    GLAccountID             INT IDENTITY(1,1) NOT NULL,
    AccountNumber           VARCHAR(20) NOT NULL,
    AccountName             NVARCHAR(150) NOT NULL,
    AccountType             CHAR(1) NOT NULL,  -- A=Asset, L=Liability, E=Equity, R=Revenue, X=Expense
    AccountSubType          VARCHAR(30) NULL,
    ParentAccountID         INT NULL,
    AccountLevel            TINYINT NOT NULL DEFAULT 1,
    IsPostable              BIT NOT NULL DEFAULT 1,
    IsBudgetable            BIT NOT NULL DEFAULT 1,
    IsStatistical           BIT NOT NULL DEFAULT 0,
    NormalBalance           CHAR(1) NOT NULL DEFAULT 'D',  -- D=Debit, C=Credit
    CurrencyCode            CHAR(3) NOT NULL DEFAULT 'USD',
    ConsolidationAccountID  INT NULL,
    IntercompanyFlag        BIT NOT NULL DEFAULT 0,
    IsActive                BIT NOT NULL DEFAULT 1,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    -- Sparse columns for rarely-populated attributes - Snowflake doesn't support SPARSE
    TaxCode                 VARCHAR(20) SPARSE NULL,
    StatutoryAccountCode    VARCHAR(30) SPARSE NULL,
    IFRSAccountCode         VARCHAR(30) SPARSE NULL,
    CONSTRAINT PK_GLAccount PRIMARY KEY CLUSTERED (GLAccountID),
    CONSTRAINT UQ_GLAccount_Number UNIQUE (AccountNumber),
    CONSTRAINT FK_GLAccount_Parent FOREIGN KEY (ParentAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT CK_GLAccount_Type CHECK (AccountType IN ('A','L','E','R','X')),
    CONSTRAINT CK_GLAccount_Balance CHECK (NormalBalance IN ('D','C'))
);
GO

-- Columnstore index for analytics - Different implementation in Snowflake
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_GLAccount_Analytics
ON Planning.GLAccount (AccountNumber, AccountName, AccountType, AccountLevel, IsActive);
GO
