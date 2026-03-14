# **Contoso Retail Extended Dataset (Retail-Dataset-V1)**

## **📌 Overview**

Welcome to the **Contoso Retail Extended Dataset**.

This project is designed to provide a rich, multi-layered relational dataset built on top of the classic Microsoft Contoso Retail DW. The primary intention of this repository is to bridge the gap between basic data sources and complex, real-world analytical environments.

It empowers **Business Intelligence (BI) Developers, Data Analysts, and SQL Practitioners** to:

* Build full-scale, end-to-end Analytics and BI projects.  
* Practice advanced data modeling and dimensional design.  
* Sharpen their SQL querying, transformation, and optimization skills in a structured environment.

By synthesizing additional operational data (like marketing spend, customer surveys, order fulfillment, and return events) and layering an analytical view model on top of the physical tables, this dataset simulates a genuine enterprise data warehouse environment.

## **🏗️ Architecture & Schema Design**

The scripts in this repository extend the base Contoso database by generating three new schemas:

* **gen (Generated/Physical Layer):** Contains the physical tables for the newly synthesized operational and transactional data (e.g., acquisitions, payments, returns).  
* **dim (Analytical Dimension Layer):** Contains curated views that serve as the dimensional lookup tables for your BI tools.  
* **fact (Analytical Fact Layer):** Contains curated views that serve as the transactional fact tables, pre-joined and formatted for immediate analytical consumption.

## **⚙️ Prerequisites**

Before you begin, ensure you have the following:

1. **Microsoft SQL Server** (Developer or Express edition is fine).  
2. **SQL Server Management Studio (SSMS)** or **Azure Data Studio**.  
3. A downloaded and restored copy of the original **Contoso Retail DW** database.

## **🚀 Execution Flow**

To set up the full extended dataset, you must execute the scripts in the exact order below.

### **Step 1: Base Database Setup**

Download the standard **Contoso Retail DW** backup file and restore it to your local SQL Server instance. Ensure the database is actively running before proceeding to the next steps.

### **Step 2: Schema Creation**

Open your SQL Server client, point it to your newly restored Contoso database, and run the schema creation script. This establishes the structural boundaries for the new data.

* 🏃 **Run:** 00\_GEN Schema/00\_create\_schemas\_v2.sql

### **Step 3: Data Synthesis (The gen schema)**

Run the following data generation scripts **in sequence**. These scripts will populate the new physical tables with realistic, synthesized business data.

* 🏃 **Run:** 00\_GEN Schema/01\_gen\_ReferenceDimensions\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/02\_gen\_CustomerAcquisition\_v2.sql *(Note: Ensure you run the latest version of this file)*  
* 🏃 **Run:** 00\_GEN Schema/03\_gen\_OrderPayment\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/04\_gen\_OrderFulfillment\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/05\_gen\_FactMarketingSpend\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/06\_gen\_FactCustomerSurvey\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/07\_gen\_OnlineReturnEvents\_v2.sql  
* 🏃 **Run:** 00\_GEN Schema/08\_gen\_PhysicalReturnEvents\_v2.sql

### **Step 4: Analytical Layer Creation (The dim and fact schemas)**

Once the physical data is generated, lay down the analytical layer. These scripts create the views that your BI tools (like Power BI or Tableau) will directly connect to.

* 🏃 **Run:** 01\_DIM Schema/09\_dim\_Views\_v1.sql  
* 🏃 **Run:** 02\_FACT Schema/10\_fact\_Views\_v2.sql

## **🧹 Teardown & Rebuild**

If you make a mistake, or if you need to reset the environment to its base Contoso state, you can easily drop all newly created objects.

To completely remove the gen, dim, and fact schemas and their underlying objects:

* 🗑️ **Run:** 00\_DROP.sql

After running the drop script, you can restart the process from **Step 2** to rebuild the dataset from scratch.

## **💡 About the Creator**

This dataset was engineered by **Waleed Mouhammed**, an Engineer, Senior Analytics Engineer, and Data Analysis Instructor with 15 years of experience turning raw data into actionable insights for decision-making.

Having managed complex operations for over a decade, Waleed understands the real-world challenges businesses face.

**Let's Connect & Collaborate:**

*I am open to collaborations and partnerships aimed at fostering data-driven growth.*

* 📧 **Email:** waleed.mouhammed@icloud.com  
* 💼 **LinkedIn:** https://www.linkedin.com/in/waleedmouhammed/  
* 🐙 **GitHub:** https://github.com/waleedmouhammed-lang