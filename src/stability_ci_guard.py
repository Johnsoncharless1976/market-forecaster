#!/usr/bin/env python3
"""
Stability Mode CI Guard
Blocks non-HOTFIX changes when STABILITY_MODE=true
"""

import os
import sys
from datetime import datetime


class StabilityCIGuard:
    """CI guard for stability mode"""
    
    def __init__(self):
        self.stability_mode = os.getenv('STABILITY_MODE', 'false').lower() == 'true'
        self.allow_hotfix = os.getenv('ALLOW_HOTFIX', 'false').lower() == 'true'
        self.ci_commit_message = os.getenv('CI_COMMIT_MESSAGE', '')
        
    def check_stability_guard(self):
        """Check if stability guard should block the build"""
        
        guard_info = {
            'stability_mode': self.stability_mode,
            'stability_block': False,
            'allow_hotfix': self.allow_hotfix,
            'is_hotfix': False,
            'block_reason': None,
            'allowed': True
        }
        
        if not self.stability_mode:
            # Stability mode off - allow all changes
            return guard_info
        
        # Check if this is a hotfix
        is_hotfix = (self.allow_hotfix or 
                    'hotfix' in self.ci_commit_message.lower() or 
                    'HOTFIX' in self.ci_commit_message)
        
        guard_info['is_hotfix'] = is_hotfix
        
        if self.stability_mode and not is_hotfix:
            # Stability mode on, not a hotfix - block
            guard_info['stability_block'] = True
            guard_info['block_reason'] = 'Stability mode active - only HOTFIX changes allowed'
            guard_info['allowed'] = False
        
        return guard_info
    
    def print_guard_status(self, guard_info):
        """Print CI guard status"""
        print("=" * 60)
        print("STABILITY MODE CI GUARD")
        print("=" * 60)
        
        print(f"STABILITY_MODE: {'ON' if guard_info['stability_mode'] else 'OFF'}")
        print(f"STABILITY_BLOCK: {'ON' if guard_info['stability_block'] else 'OFF'}")
        print(f"ALLOW_HOTFIX: {'YES' if guard_info['allow_hotfix'] else 'NO'}")
        print(f"IS_HOTFIX: {'YES' if guard_info['is_hotfix'] else 'NO'}")
        
        if guard_info['stability_block']:
            print(f"BLOCK_REASON: {guard_info['block_reason']}")
            print("")
            print("X BUILD BLOCKED")
            print("   Stability mode is active. Only HOTFIX changes are allowed.")
            print("   To override: Set ALLOW_HOTFIX=true or include 'hotfix' in commit message")
            print("")
            print("   Allowed during stability mode:")
            print("   - Emergency bug fixes")
            print("   - Critical security patches")
            print("   - Email/notification fixes")
            print("   - Batch system repairs")
            print("")
            print("   Blocked during stability mode:")
            print("   - New features")
            print("   - Dashboard changes")
            print("   - Algorithm modifications")
            print("   - Configuration updates")
        else:
            print("")
            print("OK BUILD ALLOWED")
            if guard_info['stability_mode']:
                print("   HOTFIX detected - proceeding despite stability mode")
            else:
                print("   Stability mode inactive - all changes allowed")
        
        print("=" * 60)
    
    def enforce_guard(self):
        """Enforce stability guard - exit with error if blocked"""
        guard_info = self.check_stability_guard()
        self.print_guard_status(guard_info)
        
        if not guard_info['allowed']:
            print("STABILITY GUARD: Build blocked by stability mode")
            sys.exit(1)
        
        return guard_info


def main():
    """Test Stability CI Guard"""
    
    print("Testing Stability CI Guard...")
    
    # Test with stability mode off
    print("\nTest 1: Stability mode OFF")
    os.environ['STABILITY_MODE'] = 'false'
    guard = StabilityCIGuard()
    result1 = guard.check_stability_guard()
    guard.print_guard_status(result1)
    
    # Test with stability mode on, no hotfix
    print("\nTest 2: Stability mode ON, no hotfix")
    os.environ['STABILITY_MODE'] = 'true'
    os.environ['ALLOW_HOTFIX'] = 'false'
    os.environ['CI_COMMIT_MESSAGE'] = 'feat: add new dashboard feature'
    guard = StabilityCIGuard()
    result2 = guard.check_stability_guard()
    guard.print_guard_status(result2)
    
    # Test with stability mode on, hotfix allowed
    print("\nTest 3: Stability mode ON, hotfix allowed")
    os.environ['STABILITY_MODE'] = 'true'
    os.environ['ALLOW_HOTFIX'] = 'true'
    os.environ['CI_COMMIT_MESSAGE'] = 'hotfix: critical email delivery bug'
    guard = StabilityCIGuard()
    result3 = guard.check_stability_guard()
    guard.print_guard_status(result3)
    
    return [result1, result2, result3]


if __name__ == '__main__':
    main()