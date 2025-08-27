"""
Unit tests for send_email backwards compatibility
Tests both 1-arg and 2-arg calling styles
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from send_email import send_email

class TestSendEmailCompatibility(unittest.TestCase):
    """Test suite for send_email backwards compatibility"""
    
    @patch('send_email.smtplib.SMTP')
    @patch('send_email.os.getenv')
    def test_send_email_two_args(self, mock_getenv, mock_smtp):
        """Test new 2-argument calling style: send_email(subject, body)"""
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SMTP_USER': 'test@example.com',
            'SMTP_PASS': 'testpass',
            'EMAIL_TO': 'recipient@example.com',
            'SMTP_SERVER': 'smtp.gmail.com',
            'SMTP_PORT': '587'
        }.get(key, default)
        
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        # Call send_email with 2 arguments
        result = send_email("Test Subject", "Test body content")
        
        # Verify success
        self.assertTrue(result)
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'testpass')
        mock_server.send_message.assert_called_once()
    
    @patch('send_email.smtplib.SMTP')
    @patch('send_email.os.getenv')
    def test_send_email_one_arg(self, mock_getenv, mock_smtp):
        """Test legacy 1-argument calling style: send_email(body)"""
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SMTP_USER': 'test@example.com', 
            'SMTP_PASS': 'testpass',
            'EMAIL_TO': 'recipient@example.com',
            'SMTP_SERVER': 'smtp.gmail.com',
            'SMTP_PORT': '587'
        }.get(key, default)
        
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        # Call send_email with 1 argument (legacy style)
        result = send_email("Test body content only")
        
        # Verify success
        self.assertTrue(result)
        
        # Verify SMTP was called correctly
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with('test@example.com', 'testpass')
        mock_server.send_message.assert_called_once()
    
    @patch('send_email.smtplib.SMTP_SSL')
    @patch('send_email.os.getenv')
    def test_send_email_ssl_port(self, mock_getenv, mock_smtp_ssl):
        """Test SSL port 465 usage (legacy mode)"""
        
        # Mock environment variables with SSL port
        mock_getenv.side_effect = lambda key, default=None: {
            'SMTP_USER': 'test@example.com',
            'SMTP_PASS': 'testpass', 
            'EMAIL_TO': 'recipient@example.com',
            'SMTP_SERVER': 'smtp.gmail.com',
            'SMTP_PORT': '465'
        }.get(key, default)
        
        # Mock SMTP SSL server
        mock_server = MagicMock()
        mock_smtp_ssl.return_value.__enter__.return_value = mock_server
        
        # Call send_email
        result = send_email("Test Subject", "Test body")
        
        # Verify success
        self.assertTrue(result)
        
        # Verify SMTP_SSL was used for port 465
        mock_smtp_ssl.assert_called_once_with('smtp.gmail.com', 465)
        mock_server.login.assert_called_once_with('test@example.com', 'testpass')
        mock_server.send_message.assert_called_once()
    
    def test_send_email_invalid_args(self):
        """Test that invalid number of arguments raises TypeError"""
        
        # Test no arguments
        with self.assertRaises(TypeError) as cm:
            send_email()
        self.assertIn("takes 1 or 2 positional arguments but 0 were given", str(cm.exception))
        
        # Test too many arguments
        with self.assertRaises(TypeError) as cm:
            send_email("arg1", "arg2", "arg3")
        self.assertIn("takes 1 or 2 positional arguments but 3 were given", str(cm.exception))
    
    @patch('send_email.os.getenv')
    def test_send_email_missing_credentials(self, mock_getenv):
        """Test handling of missing SMTP credentials"""
        
        # Mock missing credentials
        mock_getenv.return_value = None
        
        # Call should return False and not crash
        result = send_email("Test Subject", "Test body")
        self.assertFalse(result)
    
    @patch('send_email.smtplib.SMTP')
    @patch('send_email.os.getenv')
    def test_send_email_html_detection(self, mock_getenv, mock_smtp):
        """Test HTML content detection"""
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SMTP_USER': 'test@example.com',
            'SMTP_PASS': 'testpass',
            'EMAIL_TO': 'recipient@example.com',
            'SMTP_SERVER': 'smtp.gmail.com',
            'SMTP_PORT': '587'
        }.get(key, default)
        
        # Mock SMTP server
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        # Test with HTML content
        html_body = "<html><body><h1>Test</h1></body></html>"
        result = send_email("HTML Test", html_body)
        
        self.assertTrue(result)
        mock_server.send_message.assert_called_once()
    
    @patch('send_email.smtplib.SMTP')
    @patch('send_email.os.getenv')
    def test_send_email_smtp_error(self, mock_getenv, mock_smtp):
        """Test handling of SMTP errors"""
        
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SMTP_USER': 'test@example.com',
            'SMTP_PASS': 'testpass',
            'EMAIL_TO': 'recipient@example.com',
            'SMTP_SERVER': 'smtp.gmail.com', 
            'SMTP_PORT': '587'
        }.get(key, default)
        
        # Mock SMTP server to raise exception
        mock_smtp.side_effect = Exception("SMTP connection failed")
        
        # Call should return False and not crash
        result = send_email("Test Subject", "Test body")
        self.assertFalse(result)

if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)