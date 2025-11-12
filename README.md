# Amazon Reviews Scalable ML Pipeline

This project implements a scalable data processing and machine learning pipeline using **PySpark** and **Dask** to analyze the **Amazon Reviews dataset**.  
It focuses on distributed feature engineering, data imputation, text embedding, and decision tree modeling at scale.

---

## 🚀 Overview

The pipeline demonstrates how to efficiently handle **large-scale datasets (30+ GB)** using distributed frameworks like Spark and Dask.  
It automates the process of loading, cleaning, transforming, and modeling data across multiple computation stages.

### Key Features
- **Data Aggregation:** Compute product-level statistics (mean ratings, review counts).  
- **Category Normalization:** Extract and flatten nested JSON categories and sales ranks.  
- **Feature Engineering:** Impute missing values, encode categories, and apply PCA for dimensionality reduction.  
- **Text Embeddings:** Train a Word2Vec model on product titles to discover word relationships.  
- **Machine Learning:** Train and tune Decision Tree models for product rating prediction.  
- **Scalability:** Optimize distributed data operations with partitioning and lazy evaluation for performance.

---

## 🧠 Tech Stack
| Category | Tools |
|-----------|--------|
| Distributed Processing | PySpark, Dask |
| Machine Learning | Scikit-learn, Spark MLlib |
| Data Manipulation | Pandas, NumPy |
| Workflow Environment | VS Code, Jupyter Notebook |
| Visualization | Plotly, Matplotlib |

---
## 📂 Project Structure

.
├── amazon_reviews_pipeline.py     # Main pipeline script with all 8 PySpark + Dask tasks
├── pa2_main.py                    # Orchestrator that initializes Spark and executes tasks
├── utilities.py                   # Helper module for data I/O and result management
│
├── data/                          # Input datasets (e.g., Amazon product metadata and reviews)
│   ├── reviews.json.gz
│   ├── metadata.json.gz
│   └── ...
│
├── results/                       # Generated JSON outputs and evaluation metrics
│   ├── compute_rating_stats.json
│   ├── analyze_related_products.json
│   └── ...
│
├── requirements.txt               # Python dependencies for reproducibility
├── README.md                      # Project documentation and usage guide
└── .gitignore                     # Files and folders ignored by Git (e.g., /data, /results, *.log)
