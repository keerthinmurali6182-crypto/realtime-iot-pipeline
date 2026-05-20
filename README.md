# Real-Time Streaming IoT Data Processing Engine (PySpark)

This framework provides an end-to-end implementation of an event-driven **Real-Time IoT Conformance Engine** built with PySpark Structured Streaming. The engine captures continuous high-velocity machinery streams, evaluates conditions on the fly, and applies sliding window transformations to isolate operational anomalies instantly.

## ⚙️ Architectural Core Competencies
* **Structured Stream Engine:** Leverages optimized continuous micro-batch processing configurations via PySpark DataFrames.
* **Bounded Sliding Windows:** Implements an enterprise temporal analytics pipeline that evaluates metrics across a 5-minute event-time window sliding every 2 minutes.
* **Fault-Tolerant Watermarking:** Integrates data-late arrival mechanisms via a 10-minute structural watermark barrier to drop stale logs automatically.

## 🚀 Deployment Instructions
```bash
# 1. Prepare environment requirements
pip install -r requirements.txt

# 2. Launch concurrently the local streaming generator and the analytics pipeline engine
python src/run_system.py