-- Connecting to the database if exists.
USE contosoRetailDW;
GO

DROP TABLE IF EXISTS gen.PhysicalReturnEvents;
GO
DROP TABLE IF EXISTS gen.OnlineReturnEvents;
GO
DROP TABLE IF EXISTS gen.FactCustomerSurvey;
GO
DROP TABLE IF EXISTS gen.FactMarketingSpend;
GO
DROP TABLE IF EXISTS gen.OrderFulfillment;
GO
DROP TABLE IF EXISTS gen.OrderPayment;
GO
DROP TABLE IF EXISTS gen.CustomerAcquisition;
GO
DROP TABLE IF EXISTS gen.DimAcquisitionChannel;
GO
DROP TABLE IF EXISTS gen.DimPaymentMethod;
GO
DROP TABLE IF EXISTS gen.DimReturnReason;
GO
DROP SCHEMA IF EXISTS gen;
GO

-- Dropping views if exists.
DROP VIEW IF EXISTS dim.vDate;
GO

DROP VIEW IF EXISTS dim.vCustomer;
GO

DROP VIEW IF EXISTS dim.vProduct;
GO

DROP VIEW IF EXISTS dim.vStore;
GO

DROP VIEW IF EXISTS dim.vPromotion;
GO

DROP VIEW IF EXISTS dim.vEmployee;
GO

DROP VIEW IF EXISTS dim.vPaymentMethod;
GO

DROP VIEW IF EXISTS dim.vAcquisitionChannel;
GO

DROP VIEW IF EXISTS dim.vCurrency;
GO

DROP VIEW IF EXISTS dim.vReturnReason;
GO

DROP VIEW IF EXISTS dim.vChannel;
GO

DROP SCHEMA IF EXISTS dim;
GO

-- Dropping fact views if exists.
DROP VIEW IF EXISTS fact.vOnlineSales;
GO

DROP VIEW IF EXISTS fact.vStoreSales;
GO

DROP VIEW IF EXISTS fact.vReturns;
GO

DROP VIEW IF EXISTS fact.vInventory;
GO

DROP VIEW IF EXISTS fact.vSalesQuota;
GO

DROP VIEW IF EXISTS fact.vExchangeRate;
GO

DROP VIEW IF EXISTS fact.vOrderFulfillment;
GO

DROP VIEW IF EXISTS fact.vCustomerSurvey;
GO

DROP VIEW IF EXISTS fact.vMarketingSpend;
GO

DROP VIEW IF EXISTS fact.vCustomerAcquisition;
GO

DROP VIEW IF EXISTS fact.vOrderPayment;
GO

DROP SCHEMA IF EXISTS fact;
GO
