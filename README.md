# Medallion-Architecture-for-Health-Data-Analytics


<img width="1370" height="555" alt="Screenshot 2026-09-04 at 5 35 28 PM" src="https://github.com/user-attachments/assets/8f2a9c61-e810-48d5-8218-b646c88e0786" />


# Creating Data Lakehouse with Health Data

## Lakehouse Architecture Following Databricks Reference

This project implements the **Medallion Architecture** with **Unity Catalog integration**:

```
Data Sources → Object Storage →  Bronze →  Silver →  Gold → Consumption Layer
(Apple Health)   (Workspace)    (Delta)    (Delta)    (Delta)  (Analytics/ML)
```

### Architecture Components Build:
- ** Data Sources**: Apple Health XML exports 
- ** Object Storage**: Databricks workspace file system  
- ** Delta Tables**: ACID transactions across Bronze/Silver/Gold layers
- ** Unity Catalog**: Centralized metadata and governance (3-level namespace)
- ** Consumption Layer**: Data Marts, Analytics, ML-ready features
