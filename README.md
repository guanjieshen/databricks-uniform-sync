# **Databricks to Snowflake Mirror**

This repository provides a utility to **synchronize Iceberg table metadata** between **Databricks Unity Catalog** and **Snowflake Horizon**. It automates the creation of Snowflake catalog integrations and tables based on metadata stored in Unity Catalog. 

---

## **Table of Contents**
1. [Overview](#overview)  
2. [Setup](#setup)  
3. [How to Use](#how-to-use)  
4. [Configuration](#configuration)  
5. [Methods](#methods)  
6. [Example Usage](#example-usage)  
7. [Logging](#logging)  
8. [Troubleshooting](#troubleshooting)  

---

## **Overview**
This utility handles the following tasks:
✅ Retrieves metadata from Databricks Unity Catalog  
✅ Maps metadata to Snowflake  
✅ Creates catalog integrations in Snowflake  
✅ Creates tables in Snowflake  
✅ Supports automatic metadata refresh and tagging  

---

## **Setup**