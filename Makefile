# ZenMarket AI - Visualization
.PHONY: viz install-viz help

help:
	@echo "Available targets:"
	@echo "  viz         - Launch Streamlit visualization app"
	@echo "  install-viz - Install visualization dependencies"
	@echo "  help        - Show this help message"

install-viz:
	@echo "📦 Installing visualization dependencies..."
	pip install -r requirements-viz.txt

viz: install-viz
	@echo "🚀 Launching ZenMarket AI Visualization..."
	@echo "📊 Open http://localhost:8501 in your browser"
	streamlit run app.py