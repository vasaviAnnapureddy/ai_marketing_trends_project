# AI Marketing Trends Project (Modules 1–4)

**Author:** Vasavi Annapureddy (Batch-4)  
**Project Type:** Infosys Springboard AI Project

---

## 📘 Overview
This repository contains all four modules of my project **“AI-Driven Marketing Content System”**.  
It automates the process of collecting, analyzing, and optimizing marketing data using AI and APIs.

| Module | Name | Description |
|--------|------|-------------|
| **1** | Data Integration | Collecting and cleaning data from YouTube and Reddit APIs, storing in Google Sheets |
| **2** | Model Training | Building ML models for sentiment and engagement prediction |
| **3** | Content Generation | Using LLMs (GPT, LLaMA) for automated marketing copy generation |
| **4** | Deployment | Deploying the complete system with dashboard and automation |

---

## 🛠 Tech Stack
- **Languages:** Python  
- **Libraries:** Pandas, NumPy, PRAW, Google API, Matplotlib  
- **Platforms:** Google Sheets API, YouTube Data API, Reddit API  
- **Environment:** VS Code, GitHub  
---
## ⚙️ How to Run Module-1
1. Open terminal in VS Code  
2. Run these commands:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r Module-1_Data_Integration/requirements.txt
   python -m Module-1_Data_Integration.scripts.run_once
