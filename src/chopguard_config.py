#!/usr/bin/env python3
"""
ChopGuard v0.2 Configuration Toggle
Allows enabling/disabling governor and adjusting thresholds
"""

import json
from pathlib import Path


class ChopGuardConfig:
    """ChopGuard v0.2 configuration management"""
    
    def __init__(self):
        self.config_file = Path('audit_exports') / 'config' / 'chopguard_v02.json'
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Default configuration
        self.default_config = {
            'governor_enabled': True,
            'tau1_chop_prob': 0.40,
            'tau2_range_proxy': 0.35, 
            'calibration_method': 'platt',
            'version': '0.2',
            'last_updated': '2025-08-29'
        }
        
    def load_config(self):
        """Load configuration from file or create default"""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                return json.load(f)
        else:
            return self.default_config.copy()
            
    def save_config(self, config):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)
            
    def toggle_governor(self, enabled=None):
        """Toggle governor on/off or set specific state"""
        config = self.load_config()
        if enabled is None:
            config['governor_enabled'] = not config['governor_enabled']
        else:
            config['governor_enabled'] = enabled
        self.save_config(config)
        return config
        
    def set_thresholds(self, tau1=None, tau2=None):
        """Set governor thresholds"""
        config = self.load_config()
        if tau1 is not None:
            config['tau1_chop_prob'] = tau1
        if tau2 is not None:
            config['tau2_range_proxy'] = tau2
        self.save_config(config)
        return config
        
    def get_dashboard_chip_config(self):
        """Get dashboard chip configuration for governor state"""
        config = self.load_config()
        
        if config['governor_enabled']:
            return {
                'text': 'Governor = ON',
                'color': '#10B981',  # Green
                'background': '#ECFDF5',
                'icon': 'shield-check',
                'tooltip': f'Dual-signal governor active (τ1={config["tau1_chop_prob"]}, τ2={config["tau2_range_proxy"]})',
                'status': 'ACTIVE'
            }
        else:
            return {
                'text': 'Governor = OFF',
                'color': '#EF4444',  # Red
                'background': '#FEF2F2', 
                'icon': 'shield-off',
                'tooltip': 'Governor disabled - using calibrated probabilities only',
                'status': 'INACTIVE'
            }


def main():
    """Demo ChopGuard configuration management"""
    config_mgr = ChopGuardConfig()
    
    # Load current config
    current = config_mgr.load_config()
    print("Current ChopGuard v0.2 Configuration:")
    print(f"  Governor Enabled: {current['governor_enabled']}")
    print(f"  τ1 (CHOP prob): {current['tau1_chop_prob']}")
    print(f"  τ2 (Range proxy): {current['tau2_range_proxy']}")
    
    # Demo dashboard chip
    chip_config = config_mgr.get_dashboard_chip_config()
    print(f"\nDashboard Chip: {chip_config['text']} ({chip_config['status']})")
    
    return current


if __name__ == '__main__':
    main()