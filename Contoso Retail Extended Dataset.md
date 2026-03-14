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

## **📘 Comprehensive Project Documentation & Requirements**

This repository isn't just a sandbox; it is a blueprint for a complete, portfolio-grade capstone project. To guide your development from raw data to executive dashboards, we have included a comprehensive suite of documentation.

You can build a full end-to-end analytics project by following the provided **Project Requirements Document**. Additionally, you will find detailed architectural summaries detailing the exact SQL engineering and Power BI development work required.

Please refer to the following attached documents to guide your project:

* **`E-Commerce Business Intelligence Project.pdf` (The Requirements):** Your master project guide. It includes a structured 6-week execution timeline and over 80 role-specific business questions spanning six executive personas (CEO, CFO, COO, CSO, CMO, and Product Manager). Use this to structure your analytical approach and dashboard development.  
* **`Contoso_BI_Document1_DatabaseArchitecture.pdf`:** A deep dive into the SQL layer. This document explains the database extension, the `gen` physical layer, the view layer (`dim`/`fact`), the Star Schema design, and the Python export pipeline necessary for BI ingestion.  
* **`Contoso_BI_Document2_PowerBI_Analytics.pdf`:** Your guide to the presentation and semantic layer. It covers Power BI data modeling, DAX analytical pipelines, VertiPaq optimization, gap closure strategies, and Z-Pattern dashboard design principles.  
* **`Contoso_BI_Document3_Appendices.pdf`:** The ultimate technical reference guide. It contains the strict script execution order, a complete column dictionary, the entity relationship map, and a full DAX formula library featuring 49 advanced measures.

## **💡 About the Creator**

This dataset was engineered by **Waleed Mouhammed**, an Engineer, Senior Analytics Engineer, and Data Analysis Instructor with 15 years of experience turning raw data into actionable insights for decision-making.

Having managed complex operations for over a decade, Waleed understands the real-world challenges businesses face.

**Let's Connect & Collaborate:**

*I am open to collaborations and partnerships aimed at fostering data-driven growth.*

* 📧 **Email:** waleed.mouhammed@icloud.com  
* 💼 **LinkedIn:** https://www.linkedin.com/in/waleedmouhammed/  
* 🐙 **GitHub:** https://github.com/waleedmouhammed-lang