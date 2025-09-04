# ml_guardrails_framework.py
#!/usr/bin/env python3
"""
Machine Learning Guardrails Framework
Keeps AI contextual reasoning within bounds of proven performance patterns
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import json

class MLGuardrails:
    def __init__(self, baseline_accuracy: float = 0.88):
        self.baseline_accuracy = baseline_accuracy
        self.performance_window = 30  # Days to track recent performance
        
        # Guardrail boundaries
        self.boundaries = {
            "max_historical_adjustment": 0.3,    # AI can't adjust historical weight by more than 30%
            "min_historical_weight": 0.4,       # Historical data must retain at least 40% weight
            "max_paradigm_confidence": 0.8,     # AI paradigm calls capped at 80% confidence
            "accuracy_floor": 0.75,             # If accuracy drops below 75%, revert to historical
            "adjustment_rate_limit": 0.05,      # Max 5% adjustment per day
            "consensus_threshold": 0.6          # Require 60%+ AI consensus for major adjustments
        }
        
        # Performance tracking
        self.performance_tracker = {
            "recent_forecasts": [],
            "ai_adjustment_history": [],
            "accuracy_trend": [],
            "guardrail_triggers": []
        }
    
    def validate_ai_adjustment(self, ai_recommendation: Dict) -> Dict:
        """
        Validate AI's recommended adjustment against guardrails
        """
        proposed_adjustment = ai_recommendation.get('historical_weight_adjustment', 0)
        ai_confidence = ai_recommendation.get('confidence', 0)
        paradigm_shift_claim = ai_recommendation.get('paradigm_shift_detected', False)
        
        validation_result = {
            "approved": True,
            "adjusted_recommendation": ai_recommendation.copy(),
            "guardrail_violations": [],
            "applied_constraints": []
        }
        
        # Check maximum historical adjustment
        if abs(proposed_adjustment) > self.boundaries["max_historical_adjustment"]:
            validation_result["guardrail_violations"].append("MAX_ADJUSTMENT_EXCEEDED")
            # Clamp to maximum allowed
            clamped_adjustment = np.sign(proposed_adjustment) * self.boundaries["max_historical_adjustment"]
            validation_result["adjusted_recommendation"]["historical_weight_adjustment"] = clamped_adjustment
            validation_result["applied_constraints"].append(f"Adjustment clamped from {proposed_adjustment:.3f} to {clamped_adjustment:.3f}")
        
        # Ensure minimum historical weight
        current_historical_weight = ai_recommendation.get('current_historical_weight', 1.0)
        new_weight = current_historical_weight + proposed_adjustment
        
        if new_weight < self.boundaries["min_historical_weight"]:
            validation_result["guardrail_violations"].append("MIN_HISTORICAL_WEIGHT_VIOLATED")
            # Adjust to maintain minimum weight
            max_allowed_reduction = current_historical_weight - self.boundaries["min_historical_weight"]
            validation_result["adjusted_recommendation"]["historical_weight_adjustment"] = -max_allowed_reduction
            validation_result["applied_constraints"].append(f"Historical weight floor applied: {self.boundaries['min_historical_weight']}")
        
        # Cap paradigm confidence
        if ai_confidence > self.boundaries["max_paradigm_confidence"]:
            validation_result["adjusted_recommendation"]["confidence"] = self.boundaries["max_paradigm_confidence"]
            validation_result["applied_constraints"].append(f"Confidence capped at {self.boundaries['max_paradigm_confidence']}")
        
        # Rate limiting
        recent_adjustments = self.get_recent_adjustments(7)  # Last 7 days
        if len(recent_adjustments) > 0:
            recent_adjustment_sum = sum(adj['adjustment'] for adj in recent_adjustments)
            if abs(recent_adjustment_sum + proposed_adjustment) > self.boundaries["adjustment_rate_limit"] * 7:
                validation_result["guardrail_violations"].append("ADJUSTMENT_RATE_LIMIT_EXCEEDED")
                validation_result["approved"] = False
                validation_result["applied_constraints"].append("Rate limit exceeded - adjustment rejected")
        
        return validation_result
    
    def check_performance_degradation(self, recent_accuracy: float) -> Dict:
        """
        Monitor if AI adjustments are degrading performance
        """
        performance_check = {
            "accuracy_acceptable": True,
            "revert_to_historical": False,
            "performance_trend": "stable"
        }
        
        # Check accuracy floor
        if recent_accuracy < self.boundaries["accuracy_floor"]:
            performance_check["accuracy_acceptable"] = False
            performance_check["revert_to_historical"] = True
            performance_check["performance_trend"] = "degrading"
            
            # Log guardrail trigger
            self.performance_tracker["guardrail_triggers"].append({
                "timestamp": datetime.now(),
                "trigger_type": "ACCURACY_FLOOR_BREACH",
                "accuracy": recent_accuracy,
                "threshold": self.boundaries["accuracy_floor"]
            })
        
        # Check trend vs baseline
        if recent_accuracy < self.baseline_accuracy - 0.05:  # More than 5% below baseline
            performance_check["performance_trend"] = "concerning"
        
        return performance_check
    
    def require_ai_consensus(self, ai_recommendations: List[Dict]) -> Dict:
        """
        Require consensus among multiple AI models before major adjustments
        """
        if len(ai_recommendations) < 2:
            return {"consensus_reached": False, "reason": "Insufficient AI models for consensus"}
        
        # Calculate agreement on paradigm shift
        paradigm_shift_votes = sum(1 for rec in ai_recommendations if rec.get('paradigm_shift_detected', False))
        paradigm_consensus = paradigm_shift_votes / len(ai_recommendations)
        
        # Calculate average confidence
        avg_confidence = np.mean([rec.get('confidence', 0) for rec in ai_recommendations])
        
        # Calculate adjustment agreement
        adjustments = [rec.get('historical_weight_adjustment', 0) for rec in ai_recommendations]
        adjustment_std = np.std(adjustments)
        adjustment_agreement = adjustment_std < 0.1  # Adjustments within 10% of each other
        
        consensus_result = {
            "consensus_reached": False,
            "paradigm_consensus": paradigm_consensus,
            "average_confidence": avg_confidence,
            "adjustment_agreement": adjustment_agreement,
            "recommended_action": "maintain_historical"
        }
        
        # Determine if consensus threshold met
        if (paradigm_consensus >= self.boundaries["consensus_threshold"] and 
            avg_confidence >= 0.6 and 
            adjustment_agreement):
            
            consensus_result["consensus_reached"] = True
            consensus_result["recommended_action"] = "apply_ai_adjustment"
            consensus_result["consensus_adjustment"] = np.mean(adjustments)
        
        return consensus_result
    
    def apply_guardrailed_adjustment(self, base_forecast: Dict, ai_recommendations: List[Dict]) -> Dict:
        """
        Master function: Apply AI adjustments within guardrail constraints
        """
        # Check for AI consensus first
        consensus_check = self.require_ai_consensus(ai_recommendations)
        
        if not consensus_check["consensus_reached"]:
            return {
                "final_forecast": base_forecast,
                "ai_adjustment_applied": False,
                "reason": "No AI consensus reached",
                "guardrail_status": "consensus_requirement_failed"
            }
        
        # Validate the consensus recommendation
        consensus_recommendation = {
            "historical_weight_adjustment": consensus_check["consensus_adjustment"],
            "confidence": consensus_check["average_confidence"],
            "paradigm_shift_detected": consensus_check["paradigm_consensus"] > 0.5
        }
        
        validation = self.validate_ai_adjustment(consensus_recommendation)
        
        if not validation["approved"]:
            return {
                "final_forecast": base_forecast,
                "ai_adjustment_applied": False,
                "reason": "Guardrail violations detected",
                "violations": validation["guardrail_violations"],
                "guardrail_status": "adjustment_rejected"
            }
        
        # Apply the validated adjustment
        adjusted_forecast = base_forecast.copy()
        historical_weight = base_forecast.get('historical_confidence', 1.0)
        
        # Apply the guardrailed adjustment
        new_historical_weight = historical_weight + validation["adjusted_recommendation"]["historical_weight_adjustment"]
        ai_weight = 1.0 - new_historical_weight
        
        # Blend historical and AI-adjusted forecasts
        if 'forecast_bias' in base_forecast:
            adjusted_forecast['historical_confidence'] = new_historical_weight
            adjusted_forecast['ai_adjustment_weight'] = ai_weight
            adjusted_forecast['adjustment_reason'] = f"AI consensus with {consensus_check['average_confidence']:.2f} confidence"
        
        # Log the adjustment
        self.performance_tracker["ai_adjustment_history"].append({
            "timestamp": datetime.now(),
            "adjustment": validation["adjusted_recommendation"]["historical_weight_adjustment"],
            "ai_confidence": validation["adjusted_recommendation"]["confidence"],
            "constraints_applied": validation["applied_constraints"]
        })
        
        return {
            "final_forecast": adjusted_forecast,
            "ai_adjustment_applied": True,
            "adjustment_magnitude": validation["adjusted_recommendation"]["historical_weight_adjustment"],
            "ai_confidence": validation["adjusted_recommendation"]["confidence"],
            "constraints_applied": validation["applied_constraints"],
            "guardrail_status": "adjustment_approved"
        }
    
    def get_recent_adjustments(self, days: int) -> List[Dict]:
        """Get recent AI adjustments for rate limiting"""
        cutoff_date = datetime.now() - timedelta(days=days)
        return [
            adj for adj in self.performance_tracker["ai_adjustment_history"]
            if adj["timestamp"] >= cutoff_date
        ]
    
    def update_performance_tracking(self, forecast_result: Dict):
        """Update performance tracking with latest forecast result"""
        self.performance_tracker["recent_forecasts"].append({
            "timestamp": datetime.now(),
            "accuracy": forecast_result.get("hit", False),
            "ai_adjusted": forecast_result.get("ai_adjustment_applied", False)
        })
        
        # Calculate rolling accuracy
        recent_forecasts = self.performance_tracker["recent_forecasts"][-self.performance_window:]
        if len(recent_forecasts) >= 10:  # Need minimum sample
            accuracy = sum(1 for f in recent_forecasts if f["accuracy"]) / len(recent_forecasts)
            self.performance_tracker["accuracy_trend"].append({
                "timestamp": datetime.now(),
                "accuracy": accuracy
            })

# Example usage
if __name__ == "__main__":
    guardrails = MLGuardrails(baseline_accuracy=0.88)
    
    # Simulate AI recommendations
    ai_recs = [
        {
            "historical_weight_adjustment": -0.15,  # AI wants to reduce historical weight by 15%
            "confidence": 0.75,
            "paradigm_shift_detected": True,
            "reasoning": "COVID response infrastructure significantly changed"
        },
        {
            "historical_weight_adjustment": -0.12,
            "confidence": 0.68, 
            "paradigm_shift_detected": True,
            "reasoning": "Market structure evolution reduces historical correlation"
        }
    ]
    
    base_forecast = {
        "forecast_bias": "Bullish",
        "historical_confidence": 1.0,
        "support_levels": [6450, 6470],
        "resistance_levels": [6530, 6550]
    }
    
    result = guardrails.apply_guardrailed_adjustment(base_forecast, ai_recs)
    
    print("GUARDRAIL FRAMEWORK RESULT:")
    print(f"AI Adjustment Applied: {result['ai_adjustment_applied']}")
    print(f"Guardrail Status: {result['guardrail_status']}")
    if result['ai_adjustment_applied']:
        print(f"Adjustment Magnitude: {result['adjustment_magnitude']:.3f}")
        print(f"Constraints Applied: {result.get('constraints_applied', 'None')}")