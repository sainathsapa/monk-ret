# 🧠 Monk-RET  

Monk-RET (**Monk Retail Insights Engine**) is an intelligent retail analytics platform powered by **LangChain**, **Streamlit**, and modern AI/ML pipelines.  
It helps businesses gain actionable insights from large-scale retail data by orchestrating data ingestion, processing, and visualization seamlessly.  

---

## ✨ Features  

- 📊 **Retail Analytics Engine** – Ingests and processes large-scale retail datasets  
- 🧩 **LangChain Orchestrator** – Modular orchestration of tasks with LLMs  
- ⚡ **Batch Data Processing** – Automated CSV ingestion & database syncing  
- 📈 **Interactive Dashboards** – Streamlit-based UI for analytics & insights  
- 🔄 **Automation** – Watchdog-powered auto-refresh for new datasets  

---

## 🚀 Getting Started  

### 1️⃣ Clone the repo
```bash
git clone https://github.com/sainathsapa/monk-ret.git
cd monk-ret
```

### 2️⃣ Install dependencies
```bash
pip install -r requirements.txt
```

### 3️⃣ Run the Watchdong 
```bash
python watchdog_.py
```

### 4️⃣ (Optional) Start the Streamlit dashboard locally
```bash
python orchestrator.py
```

---

## 🛠️ Tech Stack  

- **Languages:** Python  
- **Frameworks:** LangChain, Streamlit, FastAPI, Flask  
- **Data:** Pandas, SQLAlchemy, MongoDB  
- **DevOps:** Docker, Watchdog, Jenkins, Terraform  
- **Visualization:** Plotly, Chart.js, Streamlit  

---

## 📊 Example Workflow  

1. Drop a new retail CSV into the `/data` folder  
2. `watchdog_.py` detects and inserts data → DB  
3. `langchain_orch.py` & `gen_insights_force.py` generate AI-powered insights  
4. Open `streamlit_app.py` → interactive analytics dashboard  

---

## 🤝 Contributing  

Contributions are welcome!  

1. Fork the repo  
2. Create a feature branch (`git checkout -b feature-new`)  
3. Commit changes (`git commit -m 'Added feature'`)  
4. Push (`git push origin feature-new`)  
5. Open a Pull Request  

