# Live System Updates - Generated 2025-09-03 16:04:15.689850
# Based on learned parameters from backtest analysis

# FORECAST BIAS ADJUSTMENT

# Apply learned bias confidence adjustments
def adjust_forecast_confidence(bias, base_confidence):
    multipliers = {
        'Bullish': 1.3,   # 58.7% accuracy
        'Bearish': 0.8,   # 41.0% accuracy  
        'Neutral': 1.0    # 49.5% accuracy
    }
    return base_confidence * multipliers.get(bias, 1.0)


# ALERT FILTERING

# Apply learned move size thresholds for alerts
def should_generate_alert(price_change_pct, forecast_confidence):
    abs_change = abs(price_change_pct)
    
    if abs_change <= 0.5:
        return forecast_confidence > 0.4  # Small moves reliable (71.1%)
    elif abs_change <= 1.5:
        return forecast_confidence > 0.6  # Medium moves less reliable (22.0%)
    else:
        return forecast_confidence > 0.8  # Large moves unreliable (30.2%)


# NEWS ATTRIBUTION REQUIREMENT

# Require news attribution for large moves based on learned patterns
def requires_news_attribution(price_change_pct, forecast_accuracy):
    abs_change = abs(price_change_pct)
    baseline_accuracy = 0.472  # Learned baseline
    
    # Large moves without news are suspicious given baseline performance
    if abs_change > 1.5 and forecast_accuracy > baseline_accuracy:
        return True  # Need news to explain outperformance
    
    return False


# LEVEL RECALIBRATION

# Recalibrate support/resistance based on learned breach patterns
def calculate_adjusted_levels(base_support, base_resistance, atr):
    # Tighten levels since breach accuracy was only 30.8%
    level_adjustment = 0.7  # Make levels 30% tighter
    
    adjusted_support = base_support + (atr * 0.3)   # Closer to price
    adjusted_resistance = base_resistance - (atr * 0.3)  # Closer to price
    
    return adjusted_support, adjusted_resistance


